
import asyncio
import aio_pika
from aio_pika import IncomingMessage, Message, RobustConnection
import aio_pika.abc
from msgspec import msgpack

from typing import Any, Awaitable, Callable

class RabbitClient:
    def __init__(self, rabbit_url: str):
        self.connection = None
        self.rabbit_url = rabbit_url
        pass

    async def start(self):
        self.connection = await aio_pika.connect_robust(url=self.rabbit_url, loop=asyncio.get_event_loop())
        self.connection.reconnect_callbacks.add(self._on_reconnect_callback)
        self.connection.close_callbacks.add(self._on_close_callback)

        self.channel = await self.connection.channel()

        await self.channel.set_qos(1)

    async def subscribe(self, queue_name: str, message_handler: Callable[[IncomingMessage], Awaitable[Any]]):
        queue = await self.channel.declare_queue(queue_name) # TODO: durable=True 
        await queue.consume(message_handler)

    async def publish(self, routing_key: str, message: dict):
        await self.channel.default_exchange.publish(
            Message(
                body=msgpack.encode(message),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key
        )

    def _on_reconnect_callback(self, connection: RobustConnection):
        self.connection = connection

    def _on_close_callback(self, _, __):
        self.connection = None
