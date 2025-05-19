from asyncio import Event
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from tgdb.application.ports.log_iterator import LogIterator
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.infrastructure.map_queuqe import MapQueuqe


@dataclass
class InMemoryLogIterator(LogIterator):
    _queque: MapQueuqe[AppliedOperator]
    _seconds_to_wait_next_operator: int | float
    _offset: LogicTime | None = field(default=None)
    _has_unpulled_operators: Event = field(default_factory=Event)

    def __post_init__(self) -> None:
        if self._queque:
            self._has_unpulled_operators.set()

    async def finite(self) -> AsyncIterator[AppliedOperator]:
        for offset in self._queque:
            yield self._queque[offset]

    async def commit(self, offset: LogicTime) -> None:
        self._offset = offset
        self._has_unpulled_operators.set()

    async def offset(self) -> LogicTime | None:
        return self._offset
