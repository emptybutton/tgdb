from abc import ABC, abstractmethod
from collections.abc import AsyncIterable

from tgdb.application.ports.log import LogOffset
from tgdb.entities.operator import AppliedOperator


class LogIterator(ABC, AsyncIterable[AppliedOperator]):
    @abstractmethod
    def finite(self) -> AsyncIterable[AppliedOperator]: ...

    @abstractmethod
    async def commit(self, offset: LogOffset, /) -> None: ...

    @abstractmethod
    async def offset(self) -> LogOffset | None: ...
