from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator


class LogSlot(ABC):
    @abstractmethod
    async def push(self, operator: AppliedOperator, /) -> None: ...

    @abstractmethod
    def __call__(self, *, block: bool) -> AsyncIterator[AppliedOperator]: ...

    @abstractmethod
    async def commit(self, offset: LogicTime) -> None: ...

    @abstractmethod
    async def offset(self) -> LogicTime | None: ...
