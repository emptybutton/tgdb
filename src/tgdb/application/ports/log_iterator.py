from abc import ABC, abstractmethod
from collections.abc import AsyncIterable

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator


class LogIterator(ABC, AsyncIterable[AppliedOperator]):
    @abstractmethod
    def finite(self) -> AsyncIterable[AppliedOperator]: ...

    @abstractmethod
    async def commit(self, offset: LogicTime, /) -> None: ...

    @abstractmethod
    async def offset(self) -> LogicTime | None: ...
