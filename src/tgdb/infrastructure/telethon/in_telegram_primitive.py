from asyncio import TaskGroup
from collections.abc import Awaitable, Generator
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Self, cast

from telethon.hints import TotalList

from tgdb.infrastructure.primitive_encoding import (
    Primitive,
    decoded_primitive_without_type,
    empty_table,
    encoded_primitive_without_type,
)
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.vacuum import AutoVacuum


@dataclass
class InTelegramPrimitive[PrimitiveT: Primitive](Awaitable[PrimitiveT | None]):
    _value_type: type[PrimitiveT]
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _chat_id: int
    _auto_vacuum: AutoVacuum

    _tasks: TaskGroup = field(init=False, default_factory=TaskGroup)
    _cached_value: PrimitiveT | None = field(init=False, default=None)

    async def __aenter__(self) -> Self:
        await self._tasks.__aenter__()
        self._tasks.create_task(self._auto_vacuum(self._chat_id))

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return await self._tasks.__aexit__(error_type, error, traceback)

    def __await__(self) -> Generator[Any, Any, PrimitiveT | None]:
        return self._get().__await__()

    async def set(self, value: PrimitiveT, /) -> None:
        client = self._pool_to_insert()

        last_message = await client.send_message(
            self._chat_id, encoded_primitive_without_type(value, empty_table),
        )

        self._auto_vacuum.update_horizon(last_message.id)

    async def _get(self) -> PrimitiveT | None:
        if self._cached_value is not None:
            return self._cached_value

        await self._refresh()

        return self._cached_value

    async def _refresh(self) -> None:
        client = self._pool_to_select()

        messages = await client.get_messages(self._chat_id, limit=1)
        messages = cast(TotalList, messages)

        if not messages:
            return

        last_message = messages[-1]

        self._auto_vacuum.update_horizon(last_message.id)
        self._cached_value = decoded_primitive_without_type(
            last_message.raw_text, empty_table, self._value_type
        )
