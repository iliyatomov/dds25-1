import asyncio
import json
import logging
import os
import uuid
import msgspec
from databases import Database
import msgspec
import sqlalchemy
from sqlalchemy import select, and_

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from infrastructure import RabbitClient, IncomingMessage
from events import OrderPlacedEvent, OrderCancelledEvent, OrderPaidEvent, InsufficientStockEvent

logging.basicConfig(level=logging.INFO)

DB_ERROR_STR = "DB error"
DATABASE_URL = os.environ['DATABASE_URL']

rabbit_client = RabbitClient(os.environ['RABBIT_URL'])
app = Quart("payment-service")

service_id = uuid.uuid4()

database = Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()
dialect = sqlalchemy.dialects.postgresql.dialect()

users_table = sqlalchemy.Table(
    "users",
    metadata,
    sqlalchemy.Column("user_id", sqlalchemy.String, primary_key=True),
    sqlalchemy.Column("credit", sqlalchemy.Integer, nullable=False),
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


class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue | None:
    query = sqlalchemy.select(users_table).where(users_table.c.user_id == user_id)
    rows = await database.fetch_all(query=query)
    if not rows:
        abort(400, f"User: {user_id} not found!")
    user_entry = rows[0]
    user_entry = UserValue(credit=user_entry['credit'])
    return user_entry


@app.before_serving
async def startup():
    await database.connect()

    async with database.transaction():
        for table in metadata.tables.values():
            schema = sqlalchemy.schema.CreateTable(table, if_not_exists=True)
            query = str(schema.compile(dialect=dialect))
            await database.execute(query=query)
            logging.info(f"Table {table.name} created")

    logging.info("Connected to the database")

    await rabbit_client.start()

    asyncio.create_task(rabbit_client.subscribe('order-placed', on_order_placed))
    asyncio.create_task(rabbit_client.subscribe('insufficient-stock', on_insufficient_stock))


@app.after_serving
async def shutdown():
    await database.disconnect()


LUA_ORDER_PLACED = """
local user_id = KEYS[1]
local total_cost = tonumber(ARGV[1])
local user_data = redis.call("GET", user_id)
if not user_data then
    return -1
end

local user = cmsgpack.unpack(user_data)

if user.credit >= total_cost then
    user.credit = user.credit - total_cost

    local updated_user_data = cmsgpack.pack(user)
    redis.call("SET", user_id, updated_user_data)
    return 0
else
    return -2
end
    """


charge_order_script = None


async def on_order_placed(message: IncomingMessage):

    app.logger.info(f"Received message: {message.body.decode('utf-8')}")


    event = msgspec.json.decode(json.loads(message.body.decode("utf-8")), type=OrderPlacedEvent)

    user_id = str(event.user_id)
    total_cost = event.total_cost

    query = users_table.select().where(users_table.c.user_id == user_id)
    row = await database.fetch_one(query)

    app.logger.info(f"Row: {row}")
    app.logger.info(f"User ID: {row['user_id']}")
    app.logger.info(f"Money: {row['credit']}")

    response = 0

    if not row:
        response = -1
    elif total_cost > row['credit']:
        response = -2

    app.logger.info(f"Response: {response}")
    
    if response == 0:
        new_credit = row['credit'] - total_cost

        query = (
            users_table.update()
            .where(users_table.c.user_id == user_id)
            .values(credit=new_credit)
        )
        await database.execute(query)

        order_paid_event = OrderPaidEvent(order_id=event.order_id, user_id=user_id, total_cost=total_cost,
                                              items=event.items,
                                              order_handling_service_id=event.order_handling_service_id)
        
        stmt = select(outbox_table).where(
            and_(
                outbox_table.c.aggregateid == event.order_id,
                outbox_table.c.type == "order-paid"
            )
        )

        result = await database.fetch_one(stmt)

        # if not result:
        event_data = {
            "id": uuid.uuid4(),
            "aggregatetype": "payment",
            "aggregateid": event.order_id,
            "type": "order-paid",
            "payload": msgspec.to_builtins(order_paid_event)
        }
        insert_event = outbox_table.insert().values(**event_data)
        await database.execute(insert_event)
        app.logger.info(f"Order paid event: {order_paid_event}")

        # await rabbit_client.publish('order-paid', order_paid_event)
    else:
        order_cancelled_event = OrderCancelledEvent(order_id=event.order_id, user_id=user_id,
                                                        order_handling_service_id=event.order_handling_service_id)
        
        stmt = select(outbox_table).where(
            and_(
                outbox_table.c.aggregateid == event.order_id,
                outbox_table.c.type == "order-cancelled"
            )
        )

        result = await database.fetch_one(stmt)

        # if not result:
        event_data = {
            "id": uuid.uuid4(),
            "aggregatetype": "payment",
            "aggregateid": event.order_id,
            "type": "order-cancelled",
            "payload": msgspec.to_builtins(order_cancelled_event)
        }
        insert_event = outbox_table.insert().values(**event_data)
        await database.execute(insert_event)

        app.logger.info(f"Order cancelled event: {order_cancelled_event}")
            # await rabbit_client.publish(f'order-cancelled-{event.order_handling_service_id}', order_cancelled_event)

    await message.ack()


LUA_INSUFFICIENT_STOCK = """
local user_id = KEYS[1]
local total_cost = tonumber(ARGV[1])
local user_data = redis.call("GET", user_id)

local user = cmsgpack.unpack(user_data)

local new_credit = user.credit + total_cost
user.credit = new_credit

local updated_user_data = cmsgpack.pack(user)

redis.call("SET", user_id, updated_user_data)

return 0
"""


revert_payment_script = None


async def on_insufficient_stock(message: IncomingMessage):
    event = msgpack.decode(message.body, type=InsufficientStockEvent)
    user_id = str(event.user_id)
    total_cost = event.total_cost

    query = users_table.select().where(users_table.c.user_id == user_id)
    row = await database.fetch_one(query)
    response = 0
    if not row:
        response = -1
    if response == 0:
        new_credit = row['credit'] + total_cost

        query = (
            users_table.update()
            .where(users_table.c.user_id == user_id)
            .values(credit=new_credit)
        )
        await database.execute(query)

        order_cancelled_event = OrderCancelledEvent(
                     order_id=event.order_id,
                     user_id=user_id,
                     order_handling_service_id=event.order_handling_service_id
                 )

        stmt = select(outbox_table).where(
            and_(
                outbox_table.c.aggregateid == event.order_id,
                outbox_table.c.type == "order-cancelled"
            )
        )

        result = await database.fetch_one(stmt)

        if not result:
            event_data = {
                "id": uuid.uuid4(),
                "aggregatetype": "payment",
                "aggregateid": event.order_id,
                "type": "order-cancelled",
                "payload": msgspec.to_builtins(order_cancelled_event)
            }
            insert_event = outbox_table.insert().values(**event_data)
            await database.execute(insert_event)
            # await rabbit_client.publish(f'order-cancelled-{event.order_handling_service_id}', order_cancelled_event)

    await message.ack()


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))

    app.logger.info(f"Creating user with ID: {key}")

    user_data = msgpack.decode(value)

    stmt = users_table.insert().values(
        user_id=key,
        credit=user_data['credit']
    )
    try:
        await database.execute(stmt)
    except Exception as e:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    # n = int(n)
    # starting_money = int(starting_money)
    # kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
    #                               for i in range(n)}
    # # try:
    # #    await db.mset(kv_pairs)
    # # except redis.exceptions.RedisError:
    # #     return abort(400, DB_ERROR_STR)
    try:
        # Generate list of dictionaries with user data
        app.logger.info(f"Batch init for {n} users with {starting_money} starting money")
        user_data = [
            {"user_id": str(i), "credit": starting_money}
            for i in range(n)
        ]

        # Perform batch insert
        stmt = users_table.insert(users_table).values(user_data)
        await database.execute(stmt)
        await database.commit()
    except Exception as e:
        await database.rollback()
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
    app.logger.info("hello world")
    try:

        query = users_table.select().where(users_table.c.user_id == user_id)
        row = await database.fetch_one(query)
        app.logger.info(f"Before Row: {row["credit"]}")
        query = (
            users_table.update()
            .where(users_table.c.user_id == user_id)
            .values(credit=user_entry.credit)
        )
        await database.execute(query)

        query = users_table.select().where(users_table.c.user_id == user_id)
        row = await database.fetch_one(query)
        app.logger.info(f"After Row: {row["credit"]}")
    except Exception as e:
        return abort(400, DB_ERROR_STR)
    
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    app.logger.info(f"Paying {amount} for user {user_id}")
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    # try:
    #     await db.set(user_id, msgpack.encode(user_entry))
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    try:
        query = (
            users_table.update()
            .where(users_table.c.user_id == user_id)
            .values(credit=user_entry.credit)
        )
        await database.execute(query)
    except Exception as e:
        return abort(400, DB_ERROR_STR)

    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
