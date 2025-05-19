from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import overload

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator, Operator


type LogOffset = LogicTime


class Log(ABC):
    @abstractmethod
    async def push_one(self, operator: Operator, /) -> AppliedOperator: ...

    @abstractmethod
    async def push_many(
        self, operators: Sequence[Operator], /
    ) -> Sequence[AppliedOperator]: ...

    @abstractmethod
    async def truncate(self, offset: LogOffset, /) -> None: ...
