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
    sqlalchemy.UniqueConstraint("aggregateid", "type", name="uq_outbox_aggregateid_type")
)


class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue | None:
    query = sqlalchemy.select(users_table).where(users_table.c.user_id == user_id)
    row = await database.fetch_one(query=query)
    
    if row is None:
        abort(400, f"User: {user_id} not found!")
        
    return UserValue(credit=row['credit'])


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


async def on_order_placed(message: IncomingMessage):
    event = msgspec.json.decode(json.loads(message.body.decode("utf-8")), type=OrderPlacedEvent)

    user_id = str(event.user_id)
    total_cost = event.total_cost

    async with database.transaction():
        event_query = select(outbox_table).where(
            and_(
                outbox_table.c.aggregateid == event.order_id + event.checkout_id,
                outbox_table.c.type.in_(["order-paid", "order-cancelled"]),
            )
        )

        sent_event = await database.fetch_one(event_query)
        if sent_event is not None:
            app.logger.info(f"on_order_placed already processed for order={event.order_id}")
        else:
            query = users_table.select().where(users_table.c.user_id == user_id).with_for_update()
            user_row = await database.fetch_one(query)

            if user_row is None or total_cost > user_row['credit']:
                order_cancelled_event = OrderCancelledEvent(order_id=event.order_id, checkout_id=event.checkout_id, user_id=user_id,
                                                                order_handling_service_id=event.order_handling_service_id)
                event_data = {
                    "id": uuid.uuid4(),
                    "aggregatetype": "payment",
                    "aggregateid": event.order_id + event.checkout_id,
                    "type": "order-cancelled",
                    "payload": msgspec.to_builtins(order_cancelled_event)
                }
                insert_event = outbox_table.insert().values(**event_data)
                await database.execute(insert_event)
            else:
                new_credit = user_row['credit'] - total_cost

                query = (
                    users_table.update()
                    .where(users_table.c.user_id == user_id)
                    .values(credit=new_credit)
                )
                await database.execute(query)

                order_paid_event = OrderPaidEvent(order_id=event.order_id, checkout_id=event.checkout_id, user_id=user_id, 
                                                    total_cost=total_cost, items=event.items,
                                                    order_handling_service_id=event.order_handling_service_id)

                event_data = {
                    "id": uuid.uuid4(),
                    "aggregatetype": "payment",
                    "aggregateid": event.order_id + event.checkout_id,
                    "type": "order-paid",
                    "payload": msgspec.to_builtins(order_paid_event)
                }
                insert_event = outbox_table.insert().values(**event_data)
                await database.execute(insert_event)
                

    await message.ack()

async def on_insufficient_stock(message: IncomingMessage):
    event = msgspec.json.decode(json.loads(message.body.decode("utf-8")), type=InsufficientStockEvent)

    user_id = str(event.user_id)
    total_cost = event.total_cost

    async with database.transaction():
        event_query = select(outbox_table).where(
            and_(
                outbox_table.c.aggregateid == event.order_id + event.checkout_id,
                outbox_table.c.type.in_(["order-cancelled"]),
            )
        )

        sent_event = await database.fetch_one(event_query)
        if sent_event is not None:
            app.logger.info(f"on_insufficient_stock already processed for order={event.order_id}")
        else:
            query = users_table.select().where(users_table.c.user_id == user_id).with_for_update()
            user_row = await database.fetch_one(query)

            if user_row is not None:
                new_credit = user_row['credit'] + total_cost

                query = (
                    users_table.update()
                    .where(users_table.c.user_id == user_id)
                    .values(credit=new_credit)
                )
                await database.execute(query)

            order_cancelled_event = OrderCancelledEvent(
                order_id=event.order_id, checkout_id=event.checkout_id,
                user_id=user_id,
                order_handling_service_id=event.order_handling_service_id
            )

            event_data = {
                "id": uuid.uuid4(),
                "aggregatetype": "payment",
                "aggregateid": event.order_id + event.checkout_id,
                "type": "order-cancelled",
                "payload": msgspec.to_builtins(order_cancelled_event)
            }
            insert_event = outbox_table.insert().values(**event_data)
            await database.execute(insert_event)

    await message.ack()


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))

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
    try:
        query = users_table.select().where(users_table.c.user_id == user_id)
        row = await database.fetch_one(query)

        query = (
            users_table.update()
            .where(users_table.c.user_id == user_id)
            .values(credit=user_entry.credit)
        )
        await database.execute(query)

    except Exception as e:
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
