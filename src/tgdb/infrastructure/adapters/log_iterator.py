from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from tgdb.application.ports.log_iterator import LogIterator
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.infrastructure.map_queuqe import MapQueuqe


@dataclass
class InMemoryLogIterator(LogIterator):
    _queque: MapQueuqe[AppliedOperator]
    _offset: LogicTime | None = field(default=None)

    async def finite(self) -> AsyncIterator[AppliedOperator]:
        offset_to_yield = self._queque.next_key(self._offset)

        while offset_to_yield is not None:
            yield self._queque[offset_to_yield]
            offset_to_yield = self._queque.next_key(self._offset)

    async def commit(self, offset: LogicTime) -> None:
        self._offset = offset

    async def offset(self) -> LogicTime | None:
        return self._offset
