from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator


class Log(ABC):
    @abstractmethod
    async def push(self, operator: AppliedOperator, /) -> None: ...

    @abstractmethod
    async def commit(self, offset: LogicTime) -> None: ...

    @abstractmethod
    def __call__(self, *, block: bool) -> AsyncIterator[AppliedOperator]: ...
