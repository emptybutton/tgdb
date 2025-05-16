from abc import abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass

from tgdb.application.ports.log import LogSlot
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.infrastructure.async_queque import AsyncQueque


@dataclass(frozen=True, unsafe_hash=False)
class AsyncQuequeLogSlot(LogSlot):
    _queque: AsyncQueque[AppliedOperator]

    async def push(self, operator: AppliedOperator, /) -> None:
        if self._queque and operator.time <= self._queque[-1].time:
            raise ValueError

        self._queque.push(operator)

    async def __call__(self, *, block: bool) -> AsyncIterator[AppliedOperator]:
        if block:
            async for operator in self._queque:
                yield operator

            return

        for operator in self._queque:
            yield operator

    @abstractmethod
    async def commit(self, offset: LogicTime) -> None: ...

    @abstractmethod
    async def offset(self) -> LogicTime | None: ...
