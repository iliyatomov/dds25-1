import logging
import os
import uuid
import asyncio

from databases import Database
import msgspec
import sqlalchemy

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from infrastructure import RabbitClient, IncomingMessage
from events import OrderPaidEvent, InsufficientStockEvent, StockReservedEvent

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
DATABASE_URL = os.environ['DATABASE_URL']

logging.basicConfig(level=logging.INFO)


app = Quart("stock-service")

# db = redis.asyncio.Redis(host=os.environ['REDIS_HOST'],
#                               port=int(os.environ['REDIS_PORT']),
#                               password=os.environ['REDIS_PASSWORD'],
#                               db=int(os.environ['REDIS_DB']))


rabbit_client = RabbitClient(os.environ['RABBIT_URL'])

# async def close_db_connection():
#     await db.close()

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
    sqlalchemy.Column("payload", sqlalchemy.JSON)
)


LUA_ORDER_PAID = """
local order_id = ARGV[1]
local user_id = ARGV[2]
local total_cost = tonumber(ARGV[3])

local item_ids = {}
local quantities = {}

for i = 4, #ARGV, 2 do
    table.insert(item_ids, ARGV[i])
    table.insert(quantities, tonumber(ARGV[i+1]))
end

if #item_ids == 0 then
    return 0
end

local stock_data = redis.call("MGET", unpack(item_ids))

if not stock_data then
    return -1
end

local updated_stock = {}
local new_values = {}

for i, data in ipairs(stock_data) do
    if not data then
        return -1
    end

    local success, stock_entry = pcall(cmsgpack.unpack, data)
    if not success then
        return -3
    end

    local new_stock = stock_entry.stock - quantities[i]
    if new_stock < 0 then
        return -2
    end

    table.insert(updated_stock, item_ids[i])
    table.insert(updated_stock, cmsgpack.pack({stock = new_stock, price = stock_entry.price}))
end

redis.call("MSET", unpack(updated_stock))

return 0
"""

reserve_stock_script = None

class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    # try:
    #     entry: bytes = await db.get(item_id)
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    # # deserialize data if it exists else return null
    # entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    # if entry is None:
    #     # if item does not exist in the database; abort
    #     abort(400, f"Item: {item_id} not found!")
    # return entry

    query = sqlalchemy.select(items_table).where(items_table.c.item_id == item_id)
    rows = await database.fetch_all(query=query)

    if not rows:
        abort(400, f"Item: {item_id} not found!")

    item_entry = rows[0]
    item_entry = StockValue(stock=item_entry['stock'], price=item_entry['price'])
    return item_entry

@app.before_serving
async def startup():
    # global reserve_stock_script
    # reserve_stock_script = db.register_script(LUA_ORDER_PAID)

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
    # await close_db_connection()
    await database.disconnect()

async def on_order_paid(message: IncomingMessage):
    event = msgpack.decode(message.body, type=OrderPaidEvent)

    flat_items = [val for item in event.items for val in item]
    # response = await reserve_stock_script(keys=[], args=[event.order_id, str(event.user_id), event.total_cost] + flat_items)
    #
    # if response < 0:
    #     insuff_event = InsufficientStockEvent(order_id=event.order_id, user_id=event.user_id, items=event.items, total_cost=event.total_cost, order_handling_service_id=event.order_handling_service_id)
    #     await rabbit_client.publish("insufficient-stock", insuff_event)
    # elif response == 0:
    #     succ_event = StockReservedEvent(order_id=event.order_id, user_id=event.user_id, items=event.items, total_cost=event.total_cost, order_handling_service_id=event.order_handling_service_id)
    #     await rabbit_client.publish(f"stock-reserved-{event.order_handling_service_id}", succ_event)
    #
    # await message.ack()


@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    # try:
    #     await db.set(key, value)
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    # try:
    #     await db.mset(kv_pairs)
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
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
    # try:
    #     await db.set(item_id, msgpack.encode(item_entry))
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    # try:
    #     await db.set(item_id, msgpack.encode(item_entry))
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
