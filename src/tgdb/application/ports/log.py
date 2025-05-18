from abc import ABC, abstractmethod

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator


type LogOffset = LogicTime


class Log(ABC):
    @abstractmethod
    async def push(self, *operators: AppliedOperator) -> None: ...

    @abstractmethod
    async def truncate(self, offset: LogOffset, /) -> None: ...
