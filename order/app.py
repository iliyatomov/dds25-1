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

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

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
)

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

async def get_order_from_db(order_id: str) -> OrderValue | None:
    query = sqlalchemy.select(
            orders_table.c.id,
            orders_table.c.paid,
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

    # await database.execute("DROP TABLE IF EXISTS items CASCADE")
    # await database.execute("DROP TABLE IF EXISTS orders CASCADE")
    # await database.execute("DROP TABLE IF EXISTS outbox CASCADE")
    
    for table in metadata.tables.values():
        schema = sqlalchemy.schema.CreateTable(table, if_not_exists=True)
        query = str(schema.compile(dialect=dialect))
        await database.execute(query=query)


    await rabbit_client.start()

    asyncio.create_task(rabbit_client.subscribe(f'stock-reserved-{service_id}', on_stock_reserved))
    asyncio.create_task(rabbit_client.subscribe(f'order-cancelled-{service_id}', on_order_cancelled))


@app.after_serving
async def shutdown():
    await database.disconnect()

async def on_stock_reserved(message: IncomingMessage):
    event = msgpack.decode(message.body, type=StockReservedEvent)

    # TODO
    # in transaction:
    # check if outbox table already has the event that this will produce on completion
    # if it does => acknowledge the message and return
    # if it does not => update the order to paid and insert the event into the outbox table; acknowledge the message

    query = orders_table.update().where(orders_table.c.id == event.order_id).values(paid=True)

    try:
        await database.execute(query)
        event_waiter.trigger_event(event.order_id, event.order_id)
    except Exception as e:
        event_waiter.trigger_event(event.order_id, None) # TODO: consider reverting order if trigger_event returns False

    await message.ack()


async def on_order_cancelled(message: IncomingMessage):
    event = msgpack.decode(message.body, type=OrderCancelledEvent)

    event_waiter.trigger_event(event.order_id, None)

    await message.ack()
    
@app.post('/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    query = orders_table.insert().values(id=key, user_id=user_id, paid=False, total_cost=0)
    await database.execute(query=query)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
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


@app.get('/find/<order_id>')
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


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    item_reply = await send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()

    async with database.transaction():
        query = orders_table.select().where(orders_table.c.id == order_id)
        order_entry = await database.fetch_one(query=query)

        if not order_entry:
            abort(400, f"Order {order_id} does not exist!")

        item_price = int(item_json["price"])
        new_total_cost = order_entry["total_cost"] + (int(quantity) * item_price)

        insert_item_query = items_table.insert().values(item_id=item_id, order_id=order_id, quantity=int(quantity))
        await database.execute(insert_item_query)

        update_cost_query = orders_table.update().where(orders_table.c.id == order_id).values(total_cost=new_total_cost)
        await database.execute(update_cost_query)

    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")

    async with database.transaction():
        order_entry = await get_order_from_db(order_id)

        update_order_status = orders_table.update().where(orders_table.c.id == order_id).values(status="checkout_started")
        await database.execute(update_order_status)
        
        event = OrderPlacedEvent(order_id=order_id, user_id=order_entry.user_id, items=order_entry.items, total_cost=order_entry.total_cost, order_handling_service_id=service_id)
        event_data = {
            "id": uuid.uuid4(),
            "aggregatetype": "order",
            "aggregateid": order_id,
            "type": "order-placed",
            "payload": msgspec.to_builtins(event)
        }
        insert_event = outbox_table.insert().values(**event_data)
        await database.execute(insert_event)

    app.logger.info(f"Order entry: {order_entry}")
   
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
