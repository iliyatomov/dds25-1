import asyncio
import logging
import os
import atexit
import random
import uuid

import redis
import requests

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from infrastructure import RabbitClient, IncomingMessage, EventWaiter
from events import StockReservedEvent, OrderPlacedEvent, OrderCancelledEvent

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Quart("order-service")

db = redis.asyncio.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

rabbit_client = RabbitClient(os.environ['RABBIT_URL'])

service_id = uuid.uuid4()
event_waiter = EventWaiter()

async def close_db_connection():
    await db.close()

atexit.register(close_db_connection)

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

async def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry

@app.before_serving
async def startup():
    await rabbit_client.start()

    asyncio.create_task(rabbit_client.subscribe(f'stock-reserved-{service_id}', on_stock_reserved))
    asyncio.create_task(rabbit_client.subscribe(f'order-cancelled-{service_id}', on_order_cancelled))


async def on_stock_reserved(message: IncomingMessage):
    event = msgpack.decode(message.body, type=StockReservedEvent)

    async def transaction_logic(pipe):
        entry = await pipe.get(event.order_id)
        entry = msgpack.decode(entry, type=OrderValue) if entry else None
        if entry is None:
            event_waiter.trigger_event(event.order_id, None) 
            return

        entry.paid = True

        pipe.multi()
        await pipe.set(event.order_id, msgpack.encode(entry))

    async with db.client() as client:
        await client.transaction(transaction_logic, event.order_id)

    event_waiter.trigger_event(event.order_id, event.order_id) # TODO: consider reverting order if trigger_event returns False

    await message.ack()


async def on_order_cancelled(message: IncomingMessage):
    event = msgpack.decode(message.body, type=OrderCancelledEvent)

    event_waiter.trigger_event(event.order_id, None)

    await message.ack()
    
@app.post('/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
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
    order_entry: OrderValue = await get_order_from_db(order_id)
    item_reply = await send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = await get_order_from_db(order_id)

    event = OrderPlacedEvent(order_id=order_id, user_id=order_entry.user_id, items=order_entry.items, total_cost=order_entry.total_cost, order_handling_service_id=service_id)
    await rabbit_client.publish('order-placed', event)

    data = await event_waiter.wait_for_event(order_id)
    if data is None:
        abort(400, "Checkout failed")

    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
