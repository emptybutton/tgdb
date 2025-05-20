from abc import ABC, abstractmethod

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator, Operator


type LogOffset = LogicTime


class Log(ABC):
    @abstractmethod
    async def push(self, operator: Operator, /) -> AppliedOperator: ...

    @abstractmethod
    async def truncate(self, offset: LogOffset, /) -> None: ...
