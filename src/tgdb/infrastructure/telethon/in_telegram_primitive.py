from abc import ABC
from asyncio import TaskGroup, sleep
from dataclasses import dataclass, field
from types import TracebackType
from typing import NoReturn, Self, cast

from telethon.types import Message

from tgdb.infrastructure.primitive_encoding import (
    Primitive,
    decoded_primitive,
    encoded_primitive,
)
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.vacuum import Vacuum


@dataclass
class InTelegramPrimitive[PrimitiveT: Primitive](ABC):
    _value_type: type[PrimitiveT]
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _chat_id: int
    _vacuum: Vacuum
    _seconds_between_vacuums: int | float

    _cached_first_chat_message_id: int | None = field(init=False, default=None)
    _cached_last_chat_message_id: int | None = field(init=False, default=None)
    _cached_value: PrimitiveT | None = field(init=False, default=None)
    _vacuum_tasks: TaskGroup = field(init=False, default_factory=TaskGroup)

    async def __aenter__(self) -> Self:
        await self._vacuum_tasks.__aenter__()

        self._vacuum_tasks.create_task(self._autovacuum())

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return await self._vacuum_tasks.__aexit__(
            error_type, error, traceback
        )

    async def set(self, value: PrimitiveT, /) -> None:
        client = self._pool_to_insert()

        self._cached_last_chat_message = await client.send_message(
            self._chat_id, encoded_primitive(value),
        )
        self._cached_value = value

    async def get(self) -> PrimitiveT | None:
        if self._cached_value is not None:
            return self._cached_value

        await self._refresh()

        return self._cached_value

    async def _refresh(self) -> None:
        client = self._pool_to_select()

        messages = await client.get_messages(
            self._chat_id, limit=1
        )

        if not messages:
            return

        last_message = cast(Message, messages[-1])

        self._cached_last_chat_message = last_message
        self._cached_value = decoded_primitive(
            last_message.message, self._value_type
        )

    async def _autovacuum(self) -> NoReturn:
        while True:
            await sleep(self._seconds_between_vacuums)
            await self._vacuum_chat()

    async def _vacuum_chat(self) -> None:
        if self._cached_last_chat_message_id is None:
            return

        self._vacuum_tasks.create_task(self._vacuum(
            self._chat_id,
            self._cached_first_chat_message_id,
            self._cached_last_chat_message_id,
        ))

        self._cached_first_chat_message_id = self._cached_last_chat_message_id
