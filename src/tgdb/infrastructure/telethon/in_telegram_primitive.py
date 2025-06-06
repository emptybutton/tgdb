from collections.abc import Awaitable, Generator
from dataclasses import dataclass, field
from typing import Any, cast

from telethon.hints import TotalList

from tgdb.infrastructure.primitive_encoding import (
    Primitive,
    decoded_primitive_without_type,
    empty_table,
    encoded_primitive_without_type,
)
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool


@dataclass
class InTelegramPrimitive[PrimitiveT: Primitive](Awaitable[PrimitiveT | None]):
    _value_type: type[PrimitiveT]
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _chat_id: int

    _cached_value: PrimitiveT | None = field(init=False, default=None)

    def __await__(self) -> Generator[Any, Any, PrimitiveT | None]:
        return self._get().__await__()

    async def set(self, value: PrimitiveT, /) -> None:
        client = self._pool_to_insert()
        await client.send_message(
            self._chat_id, encoded_primitive_without_type(value, empty_table),
        )
        self._cached_value = value

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
        self._cached_value = decoded_primitive_without_type(
            last_message.raw_text, empty_table, self._value_type
        )
