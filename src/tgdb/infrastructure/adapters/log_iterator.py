from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from tgdb.application.ports.log import LogOffset
from tgdb.application.ports.log_iterator import LogIterator
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.infrastructure.map_queuqe import MapQueuqe
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.in_telegram_primitive import (
    InTelegramPrimitive,
)
from tgdb.infrastructure.telethon.lazy_message_map import LazyMessageMap


@dataclass
class InMemoryLogIterator(LogIterator):
    _queque: MapQueuqe[AppliedOperator]
    _offset: LogOffset | None = field(default=None)

    async def finite(self) -> AsyncIterator[AppliedOperator]:
        offset_to_yield = self._queque.next_key(self._offset)

        while offset_to_yield is not None:
            yield self._queque[offset_to_yield]
            offset_to_yield = self._queque.next_key(self._offset)

    async def commit(self, offset: LogOffset) -> None:
        self._offset = offset

    async def offset(self) -> LogOffset | None:
        return self._offset


@dataclass(frozen=True, unsafe_hash=False)
class InTelegramLogIterator(LogIterator, ABC):
    _pool_to_pull: TelegramClientPool
    _heap_id: int
    _message_map: LazyMessageMap
    _in_tg_offset: InTelegramPrimitive[LogOffset]

    async def commit(self, offset: LogOffset) -> None:
        await self._in_tg_offset.set(offset)

    async def offset(self) -> LogOffset | None:
        return await self._in_tg_offset.get()
