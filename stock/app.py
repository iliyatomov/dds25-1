import logging
import os
import uuid
import asyncio

from databases import Database
import msgspec
import sqlalchemy
import json

from sqlalchemy import select, and_

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from infrastructure import RabbitClient, IncomingMessage
from events import OrderPaidEvent, InsufficientStockEvent, StockReservedEvent

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
DATABASE_URL = os.environ['DATABASE_URL']

logging.basicConfig(level=logging.INFO)

app = Quart("stock-service")

rabbit_client = RabbitClient(os.environ['RABBIT_URL'])

database = Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()
dialect = sqlalchemy.dialects.postgresql.dialect()

items_table = sqlalchemy.Table(
    "items",
    metadata,
    sqlalchemy.Column("item_id", sqlalchemy.String, primary_key=True),
    sqlalchemy.Column("stock", sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("price", sqlalchemy.Integer, nullable=False)
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


class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue | None:
    query = sqlalchemy.select(items_table).where(items_table.c.item_id == item_id)
    item_row = await database.fetch_one(query=query)

    if item_row is None:
        abort(400, f"Item: {item_id} not found!")

    return StockValue(stock=item_row['stock'], price=item_row['price'])


@app.before_serving
async def startup():
    await database.connect()

    async with database.transaction():
        for table in metadata.tables.values():
            schema = sqlalchemy.schema.CreateTable(table, if_not_exists=True)
            query = str(schema.compile(dialect=dialect))
            await database.execute(query=query)
            logging.info(f"Table {table.name} created.")

    logging.info("Database connected and tables created.")

    await rabbit_client.start()

    asyncio.create_task(rabbit_client.subscribe(f'order-paid', on_order_paid))


@app.after_serving
async def shutdown():
    await database.disconnect()

async def on_order_paid(message: IncomingMessage):
    event = msgspec.json.decode(json.loads(message.body.decode("utf-8")), type=OrderPaidEvent)

    order_id = event.order_id
    user_id = str(event.user_id)
    total_cost = event.total_cost
    items = event.items

    item_ids = [item[0] for item in items]
    quantity_map = {item[0]: item[1] for item in items}

    async with database.transaction():
        event_query = select(outbox_table).where(
            and_(
                outbox_table.c.aggregateid == event.order_id,
                outbox_table.c.type.in_(["stock-reserved", "insufficient-stock"]),
            )
        )

        sent_event = await database.fetch_one(event_query)
        if sent_event is not None:
            app.logger.info(f"on_order_paid already processed for order={event.order_id}")
        else:
            query = items_table.select().where(items_table.c.item_id.in_(item_ids)).with_for_update()
            item_rows = await database.fetch_all(query=query)

            insufficient = len(item_rows) != len(item_ids)

            if not insufficient:
                new_stocks = {}

                for row in item_rows:
                    item_id = row['item_id']
                    current_stock = row['stock']
                    quantity_needed = quantity_map[item_id]

                    new_stock = current_stock - quantity_needed
                    if new_stock < 0:
                        insufficient = True
                        break
                    new_stocks[item_id] = new_stock

            if not insufficient:
                for item_id, new_stock in new_stocks.items():
                    update_query = (
                        items_table.update()
                        .where(items_table.c.item_id == item_id)
                        .values(stock=new_stock)
                    )
                    await database.execute(update_query)

                stock_reserved_event = StockReservedEvent(
                    order_id=order_id,
                    checkout_id=event.checkout_id,
                    user_id=user_id,
                    items=items,
                    total_cost=total_cost,
                    order_handling_service_id=event.order_handling_service_id
                )

                insert_event = outbox_table.insert().values(
                    id=uuid.uuid4(),
                    aggregatetype="stock",
                    aggregateid=order_id + event.checkout_id,
                    type="stock-reserved",
                    payload=msgspec.to_builtins(stock_reserved_event)
                )
                await database.execute(insert_event)
            else:
                insufficient_stock_event = InsufficientStockEvent(
                    order_id=order_id,
                    checkout_id=event.checkout_id,
                    user_id=user_id,
                    items=items,
                    total_cost=total_cost,
                    order_handling_service_id=event.order_handling_service_id
                )

                insert_event = outbox_table.insert().values(
                    id=uuid.uuid4(),
                    aggregatetype="stock",
                    aggregateid=order_id + event.checkout_id,
                    type="insufficient-stock",
                    payload=msgspec.to_builtins(insufficient_stock_event)
                )
                await database.execute(insert_event)

    await message.ack()


@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))

    item_data = msgpack.decode(value)

    stmt = items_table.insert().values(
        item_id=key,
        stock=item_data['stock'],
        price=item_data['price']
    )

    try:
        await database.execute(stmt)
    except Exception as e:
        app.logger.error(f"Error creating item: {e}")
        return abort(400, DB_ERROR_STR)

    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    try:
        item_data = [{"item_id": str(i), "stock": starting_stock, "price": item_price} for i in range(n)]

        stmt = items_table.insert().values(item_data)
        await database.execute(stmt)
        await database.commit()
    except Exception as e:
        app.logger.error(f"Error creating items: {e}")
        return abort(400, DB_ERROR_STR)

    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    item_entry: StockValue = await get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)

    try:
        update_query = (
            items_table.update()
            .where(items_table.c.item_id == item_id)
            .values(stock=item_entry.stock)
        )
        await database.execute(update_query)
    except Exception as e:
        app.logger.error(f"Error updating item: {e}")
        return abort(400, DB_ERROR_STR)

    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")

    try:
        update_query = (
            items_table.update()
            .where(items_table.c.item_id == item_id)
            .values(stock=item_entry.stock)
        )
        await database.execute(update_query)
    except Exception as e:
        app.logger.error(f"Error updating item: {e}")
        return abort(400, DB_ERROR_STR)

    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
