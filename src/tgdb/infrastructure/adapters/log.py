from asyncio import Event
from collections import OrderedDict
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from time import sleep
from typing import Self, override

from tgdb.application.ports.log import Log
from tgdb.application.ports.log_iterator import LogIterator
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.infrastructure.map_queuqe import MapQueuqe


@dataclass
class InMemoryLog(Log):
    _queque: MapQueuqe[LogicTime, AppliedOperator]

    async def push(self, operator: AppliedOperator) -> None:
        self._queque.push(operator.time, operator)

    @override
    async def truncate(self, offset_to_truncate: LogicTime, /) -> None:
        for index, offset in enumerate(self._queque):
            if offset == offset_to_truncate:
                self._queque.pull(count=index + 1)
                break

            if offset > offset_to_truncate:
                self._queque.pull(count=index)
                break


@dataclass
class InMemoryLogIterator(LogIterator):
    _queque: MapQueuqe[LogicTime, AppliedOperator]
    _seconds_to_wait_next_operator: int | float
    _offset: LogicTime | None = field(default=None)
    _has_unpulled_operators: Event = field(default_factory=Event)

    def __post_init__(self) -> None:
        if self._queque:
            self._has_unpulled_operators.set()

    # async def block(self) -> AsyncIterator[AppliedOperator]:
    #     while True: ...

    async def __anext__(self) -> AppliedOperator:
        while True:
            next_offset_ = self._next_offset()

            while next_offset_ is not None:
                await sleep(self._seconds_to_wait_next_operator)
                next_offset_ = self._next_offset()

            yield self._queque[next_offset_]

    async def __aiter__(self) -> AsyncIterator[AppliedOperator]:
        while True:
            if self._offset is not None and self._offset >= max(self._queque):
                return

            if self._offset is None:
                next_operand_offset = min(self._queque)
            else:
                next_operand_offset = self._offset + 1

            next_operand = None

            while (
                next_operand_offset < max(self._queque)
                or next_operand is not None
            ):
                next_operand = self._queque.get(next_operand_offset)
                next_operand_offset += 1

            self._offset = next_operand_offset - 1

            if next_operand is not None:
                yield next_operand

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
