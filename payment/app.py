import asyncio
import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from infrastructure import RabbitClient, IncomingMessage
from events import OrderPlacedEvent, OrderCancelledEvent, OrderPaidEvent, InsufficientStockEvent

DB_ERROR_STR = "DB error"


rabbit_client = RabbitClient(os.environ['RABBIT_URL'])
app = Quart("payment-service")

service_id = uuid.uuid4()

db = redis.asyncio.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


async def close_db_connection():
    await db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.before_serving
async def startup():
    await rabbit_client.start()

    asyncio.create_task(rabbit_client.subscribe(f'order-placed', on_order_placed))
    asyncio.create_task(rabbit_client.subscribe(f'insufficient-stock', on_insufficient_stock))


async def on_order_placed(message: IncomingMessage):
    event = msgpack.decode(message.body, type=OrderPlacedEvent)
    user_id = event.user_id
    total_cost = event.total_cost

    async def transaction_logic(pipe):
        user_credit = await pipe.get(user_id)
        user_credit = msgpack.decode(user_credit, type=UserValue).credit if user_credit else None

        if user_credit is not None and user_credit >= total_cost:
            new_credit = user_credit - total_cost

            pipe.multi()
            await pipe.set(user_id, msgpack.encode(UserValue(credit=new_credit)))

            order_paid_event = OrderPaidEvent(order_id=event.order_id, user_id=user_id, total_cost=total_cost, items=event.items, order_handling_service_id=event.order_handling_service_id)
            await rabbit_client.publish('order-paid', order_paid_event)
        else:
            order_cancelled_event = OrderCancelledEvent(order_id=event.order_id, user_id=user_id, order_handling_service_id=event.order_handling_service_id)
            await rabbit_client.publish(f'order-cancelled-{event.order_handling_service_id}', order_cancelled_event)

    async with db.client() as client:
        await client.transaction(transaction_logic, user_id)

    await message.ack()

async def on_insufficient_stock(message: IncomingMessage):
    event = msgpack.decode(message.body, type=InsufficientStockEvent)
    user_id = event.user_id
    total_cost = event.total_cost

    async def transaction_logic(pipe):
        user_credit = await pipe.get(user_id)
        user_credit = msgpack.decode(user_credit, type=UserValue).credit if user_credit else None

        new_credit = user_credit + total_cost

        pipe.multi()
        await pipe.set(user_id, msgpack.encode(UserValue(credit=new_credit)))

        order_cancelled_event = OrderCancelledEvent(order_id=event.order_id, user_id=user_id, order_handling_service_id=event.order_handling_service_id)
        await rabbit_client.publish(f'order-cancelled-{event.order_handling_service_id}', order_cancelled_event)

    async with db.client() as client:
        await client.transaction(transaction_logic, user_id)

    await message.ack()


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
       await db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    user_entry: UserValue = await get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
