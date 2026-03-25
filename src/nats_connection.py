from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Awaitable, Callable, Optional

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription


class NatsConnection:
    def __init__(self, host: str, port: int, client_name: str, token: str) -> None:
        self.host = host
        self.port = port
        self.client_name = client_name
        self.token = token
        self._client: Optional[Client] = None

    @property
    def client(self) -> Client:
        if not self._client:
            raise RuntimeError("NATS-Connection failed.")
        return self._client

    async def connect(self) -> None:
        if self._client and self._client.is_connected:
            return

        self._client = Client()
        await self._client.connect(
            servers=[f"nats://{self.host}:{self.port}"],
            name=self.client_name,
            token=self.token,
            allow_reconnect=True,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
            inbox_prefix=f"_INBOX.{self.client_name}",
        )

    async def close(self) -> None:
        if self._client and self._client.is_connected:
            await self._client.drain()
            await self._client.close()
            self._client = None

    async def subscribe(
        self,
        subject: str,
        queue: str | None = None,
        callback: Callable[[Msg], Awaitable[None]] | None = None,
    ) -> Subscription:
        await self.connect()
        if callback:
            async def _cb(msg: Msg) -> None:
                result = callback(msg)
                if asyncio.iscoroutine(result):
                    await result

            return await self.client.subscribe(subject, queue, cb=_cb)
        return await self.client.subscribe(subject, queue)

    async def publish(self, subject: str, payload: bytes, reply_to: str | None = None) -> None:
        await self.connect()
        await self.client.publish(subject, payload, reply=reply_to)

    async def request(self, subject: str, payload: bytes, timeout: float = 2.0) -> Msg:
        await self.connect()
        return await self.client.request(subject, payload, timeout=timeout)

    async def flush(self, timeout: float = 1.0) -> None:
        await self.connect()
        await self.client.flush(timeout=timeout)


@asynccontextmanager
async def open_nats_connection(host: str, port: int, client_name: str, token: str):
    conn = NatsConnection(host, port, client_name, token)
    await conn.connect()
    try:
        yield conn
    finally:
        await conn.close()
