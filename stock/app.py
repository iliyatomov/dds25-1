import logging
import os
import atexit
import uuid
import asyncio

import redis

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from infrastructure import RabbitClient, IncomingMessage
from events import OrderPaidEvent, InsufficientStockEvent, StockReservedEvent

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

app = Quart("stock-service")

db = redis.asyncio.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


rabbit_client = RabbitClient(os.environ['RABBIT_URL'])

async def close_db_connection():
    await db.close()

atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = await db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry

@app.before_serving
async def startup():
    await rabbit_client.start()

    asyncio.create_task(rabbit_client.subscribe(f'order-paid', on_order_paid))


async def on_order_paid(message: IncomingMessage):
    event = msgpack.decode(message.body, type=OrderPaidEvent)

    async with db.pipeline(transaction=True) as pipe:
        stock_values = {}

        for item_id, quantity in event.items:
            stock_entry = await get_item_from_db(item_id)
            if not stock_entry:
                insuff_event = InsufficientStockEvent(order_id=event.order_id, user_id=event.user_id, items=event.items,
                                                      total_cost=event.total_cost,
                                                      order_handling_service_id=event.order_handling_service_id)
                
                await rabbit_client.publish("insufficient-stock", insuff_event)
                await message.ack()
                return

            new_stock = stock_entry.stock - quantity
            if new_stock < 0:
                app.logger.error(f"Insufficient stock for item {item_id}!")
                insuff_event = InsufficientStockEvent(order_id=event.order_id, user_id=event.user_id, items=event.items,
                                                      total_cost=event.total_cost,
                                                      order_handling_service_id=event.order_handling_service_id)

                await rabbit_client.publish("insufficient-stock", insuff_event)
                await message.ack()
                return

            stock_values[item_id] = new_stock

        try:
            for item_id, new_stock in stock_values.items():
                stock_entry = await get_item_from_db(item_id)
                stock_entry.stock = new_stock
                pipe.set(item_id, msgpack.encode(stock_entry))

            await pipe.execute()
            succ_event = StockReservedEvent(order_id=event.order_id, user_id=event.user_id, items=event.items,
                                                total_cost=event.total_cost,
                                                order_handling_service_id=event.order_handling_service_id)

            await rabbit_client.publish(f"stock-reserved-{event.order_handling_service_id}", succ_event)
            await message.ack()

        except redis.exceptions.RedisError as e:
            return abort(400, DB_ERROR_STR)



@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        await db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except redis.exceptions.RedisError:
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
        await db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
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
        await db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
