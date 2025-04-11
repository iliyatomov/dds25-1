import asyncio
import logging
import os
import random
import uuid
import json 

from databases import Database
import msgspec
import sqlalchemy
import requests

from msgspec import Struct
from quart import Quart, jsonify, abort, Response
from sqlalchemy import select, and_

from infrastructure import RabbitClient, IncomingMessage, EventWaiter
from events import StockReservedEvent, OrderPlacedEvent, OrderCancelledEvent

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
DATABASE_URL = os.environ['DATABASE_URL']

app = Quart("order-service")

rabbit_client = RabbitClient(os.environ['RABBIT_URL'])

service_id = uuid.uuid4()
event_waiter = EventWaiter()


database = Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()
dialect = sqlalchemy.dialects.postgresql.dialect()

orders_table = sqlalchemy.Table(
    "orders",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.String, primary_key=True),
    sqlalchemy.Column("user_id", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("total_cost", sqlalchemy.Integer, default=0),
    sqlalchemy.Column("paid", sqlalchemy.Boolean, default=False),
    sqlalchemy.Column("status", sqlalchemy.String, default="none"),
)

items_table = sqlalchemy.Table(
    "items",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("item_id", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("order_id", sqlalchemy.String, sqlalchemy.ForeignKey("orders.id")),
    sqlalchemy.Column("quantity", sqlalchemy.Integer, default=0),
)

outbox_table = sqlalchemy.Table(
    "outbox",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False),
    sqlalchemy.Column("aggregatetype", sqlalchemy.String(255), nullable=False),
    sqlalchemy.Column("aggregateid", sqlalchemy.String(255), nullable=False, index=True),
    sqlalchemy.Column("type", sqlalchemy.String(255), nullable=False),
    sqlalchemy.Column("payload", sqlalchemy.JSON),
    sqlalchemy.UniqueConstraint("aggregateid", "type", name="uq_outbox_aggregateid_type")
)

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

async def get_order_from_db(order_id: str) -> OrderValue | None:
    query = select(
        orders_table.c.id,
        orders_table.c.paid,
        orders_table.c.status,
        orders_table.c.user_id,
        orders_table.c.total_cost,
        items_table.c.item_id,
        items_table.c.quantity
    ).where(orders_table.c.id == order_id).outerjoin(items_table, orders_table.c.id == items_table.c.order_id)
    rows = await database.fetch_all(query)

    if len(rows) == 0:
        abort(400, f"Order: {order_id} not found!")

    items_list = [(row["item_id"], row["quantity"]) for row in rows if "item_id" in row and row["item_id"] is not None]

    order_value = OrderValue(
        paid=rows[0]["paid"],
        items=items_list,
        user_id=rows[0]["user_id"],
        total_cost=rows[0]["total_cost"]
    )

    return order_value


@app.before_serving
async def startup():
    await database.connect();
    
    for table in metadata.tables.values():
        schema = sqlalchemy.schema.CreateTable(table, if_not_exists=True)
        query = str(schema.compile(dialect=dialect))
        await database.execute(query=query)

    await rabbit_client.start()

    asyncio.create_task(rabbit_client.subscribe(f'stock-reserved', on_stock_reserved))
    asyncio.create_task(rabbit_client.subscribe(f'order-cancelled', on_order_cancelled))

    asyncio.create_task(rabbit_client.subscribe(f'order-completed-{service_id}', on_completed_reserved_response))
    asyncio.create_task(rabbit_client.subscribe(f'order-cancelled-{service_id}', on_order_cancelled_response))


@app.after_serving
async def shutdown():
    await database.disconnect()

async def on_stock_reserved(message: IncomingMessage):
    event = msgspec.json.decode(json.loads(message.body.decode("utf-8")), type=StockReservedEvent)

    async with database.transaction():
        event_query = select(outbox_table).where(
            and_(
                outbox_table.c.aggregateid == event.order_id + event.checkout_id,
                outbox_table.c.type.in_([f"order-completed-{event.order_handling_service_id}"]),
            )
        )

        sent_event = await database.fetch_one(event_query)
        if sent_event is not None:
            app.logger.info(f"on_stock_reserved already processed for order={event.order_id}")
        else:
            query = select().where(orders_table.c.id == event.order_id).with_for_update()
            item_row = await database.fetch_one(query=query)
            if item_row is None:
                app.logger.info(f"Order {event.order_id} not found")
            else:
                update_order_status = orders_table.update().where(orders_table.c.id == event.order_id).values(paid=True, status="paid")
                await database.execute(update_order_status)

                event_data = {
                    "id": uuid.uuid4(),
                    "aggregatetype": "order",
                    "aggregateid": event.order_id + event.checkout_id,
                    "type": f"order-completed-{event.order_handling_service_id}",
                    "payload": msgspec.to_builtins(event)
                }
                insert_event = outbox_table.insert().values(**event_data)
                await database.execute(insert_event)

    await message.ack()


async def on_completed_reserved_response(message: IncomingMessage):
    # the message handler does not interact with the database, so we can just acknowledge the message and return the response to the user
    # if this fails, we won't be able to send the response to the user anyway, so we can just ignore it
    event = msgspec.json.decode(json.loads(message.body.decode("utf-8")), type=StockReservedEvent)

    event_waiter.trigger_event(event.order_id, event.order_id)

    await message.ack()


async def on_order_cancelled(message: IncomingMessage):
    event = msgspec.json.decode(json.loads(message.body.decode("utf-8")), type=OrderCancelledEvent)

    async with database.transaction():
        event_query = select(outbox_table).where(
            and_(
                outbox_table.c.aggregateid == event.order_id + event.checkout_id,
                outbox_table.c.type.in_([f"order-cancelled-{event.order_handling_service_id}"]),
            )
        )

        sent_event = await database.fetch_one(event_query)
        if sent_event is not None:
            app.logger.info(f"on_order_cancelled already processed for order={event.order_id}")
        else:
            query = select().where(orders_table.c.id == event.order_id).with_for_update()
            item_row = await database.fetch_one(query=query)
            if item_row is None:
                app.logger.info(f"Order {event.order_id} not found")
            else:
                update_order_status = orders_table.update().where(orders_table.c.id == event.order_id).values(status="none")
                await database.execute(update_order_status)

                event_data = {
                    "id": uuid.uuid4(),
                    "aggregatetype": "order",
                    "aggregateid": event.order_id + event.checkout_id,
                    "type": f"order-cancelled-{event.order_handling_service_id}",
                    "payload": msgspec.to_builtins(event)
                }
                insert_event = outbox_table.insert().values(**event_data)
                await database.execute(insert_event)

    await message.ack()
    

async def on_order_cancelled_response(message: IncomingMessage):
    # the message handler does not interact with the database, so we can just acknowledge the message and return the response to the user
    # if this fails, we won't be able to send the response to the user anyway, so we can just ignore it
    event = msgspec.json.decode(json.loads(message.body.decode("utf-8")), type=OrderCancelledEvent)

    event_waiter.trigger_event(event.order_id, None)

    await message.ack()


@app.post('/orders/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    query = orders_table.insert().values(id=key, user_id=user_id, paid=False, total_cost=0, status="none")
    await database.execute(query=query)
    return jsonify({'order_id': key})


@app.post('/orders/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    orders_to_insert = []
    items_to_insert = []

    for _ in range(n):
        order_id = str(uuid.uuid4())
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)

        orders_to_insert.append(
            {"id": order_id, "user_id": str(user_id), "paid": False, "total_cost": 2 * item_price}
        )

        items_to_insert.append({"item_id": str(item1_id), "order_id": order_id, "quantity": 1})
        items_to_insert.append({"item_id": str(item2_id), "order_id": order_id, "quantity": 1})

    if orders_to_insert:
        await database.execute_many(query=orders_table.insert(), values=orders_to_insert)

    if items_to_insert:
        await database.execute_many(query=items_table.insert(), values=items_to_insert)

    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/orders/find/<order_id>')
async def find_order(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


async def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/orders/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    item_reply = await send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()

    async with database.transaction():
        query = orders_table.select().where(orders_table.c.id == order_id).with_for_update()
        order_entry = await database.fetch_one(query=query)

        if order_entry is None:
            abort(400, f"Order {order_id} does not exist!")

        item_price = int(item_json["price"])
        new_total_cost = order_entry["total_cost"] + (int(quantity) * item_price)

        insert_item_query = items_table.insert().values(item_id=item_id, order_id=order_id, quantity=int(quantity))
        await database.execute(insert_item_query)

        update_cost_query = orders_table.update().where(orders_table.c.id == order_id).values(total_cost=new_total_cost)
        await database.execute(update_cost_query)

    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

@app.post('/orders/checkout/<order_id>')
async def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")

    async with database.transaction():
        query = select(
            orders_table.c.id,
            orders_table.c.paid,
            orders_table.c.status,
            orders_table.c.user_id,
            orders_table.c.total_cost,
            items_table.c.item_id,
            items_table.c.quantity
        ).where(orders_table.c.id == order_id).outerjoin(items_table, orders_table.c.id == items_table.c.order_id)
        rows = await database.fetch_all(query)

        if len(rows) == 0:
            abort(400, f"Order: {order_id} not found!")
        if rows[0]["paid"]:
            return Response("Checkout successful", status=200)
        if rows[0]["status"] != "none":
            abort(400, f"Order: {order_id} already in progress!")

        items_list = [(row["item_id"], row["quantity"]) for row in rows if "item_id" in row and row["item_id"] is not None]

        order_value = OrderValue(
            paid=rows[0]["paid"],
            items=items_list,
            user_id=rows[0]["user_id"],
            total_cost=rows[0]["total_cost"]
        )

        update_order_status = orders_table.update().where(orders_table.c.id == order_id).values(status="checkout_started")
        await database.execute(update_order_status)
        
        checkout_id = str(uuid.uuid4())

        event = OrderPlacedEvent(order_id=order_id, checkout_id=checkout_id, user_id=order_value.user_id, items=order_value.items, total_cost=order_value.total_cost, order_handling_service_id=service_id)
        event_data = {
            "id": uuid.uuid4(),
            "aggregatetype": "order",
            "aggregateid": order_id + checkout_id,
            "type": "order-placed",
            "payload": msgspec.to_builtins(event)
        }
        insert_event = outbox_table.insert().values(**event_data)
        await database.execute(insert_event)
   
    data = await event_waiter.wait_for_event(order_id)
    if data is None:
        abort(400, "Checkout failed")

    return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
