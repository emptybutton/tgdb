from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator


class Log(ABC):
    @abstractmethod
    async def push(self, operator: AppliedOperator, /) -> None: ...

    @abstractmethod
    async def truncate(self, offset: LogicTime, /) -> None: ...
