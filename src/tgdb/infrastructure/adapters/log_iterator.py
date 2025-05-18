from asyncio import Event, sleep
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Self

from tgdb.application.ports.log_iterator import LogIterator
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.infrastructure.map_queuqe import MapQueuqe


@dataclass
class InMemoryLogIterator(LogIterator):
    _queque: MapQueuqe[LogicTime, AppliedOperator]
    _seconds_to_wait_next_operator: int | float
    _offset: LogicTime | None = field(default=None)
    _has_unpulled_operators: Event = field(default_factory=Event)

    def __post_init__(self) -> None:
        if self._queque:
            self._has_unpulled_operators.set()

    async def finite(self) -> AsyncIterator[AppliedOperator]:
        next_offset_ = self._next_offset()

        while next_offset_ is not None:
            yield self._queque[next_offset_]
            next_offset_ = self._next_offset()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> AppliedOperator:
        next_offset_ = self._next_offset()

        while next_offset_ is None:
            await sleep(self._seconds_to_wait_next_operator)
            next_offset_ = self._next_offset()

        return self._queque[next_offset_]

    async def commit(self, offset: LogicTime) -> None:
        self._offset = offset
        self._has_unpulled_operators.set()

    async def offset(self) -> LogicTime | None:
        return self._offset

    def _next_offset(self) -> LogicTime | None:
        if not self._queque:
            return None

        if self._offset is None:
            return min(self._queque)

        if self._offset >= max(self._queque):
            return None

        next_offset = self._offset + 1

        while next_offset not in self._queque:
            next_offset += 1

        return next_offset
