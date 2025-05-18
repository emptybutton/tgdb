from abc import ABC, abstractmethod
from collections.abc import AsyncIterator


class SyncQueque[ValueT](ABC):
    @abstractmethod
    async def push(self, *values: ValueT) -> None: ...

    @abstractmethod
    async def sync(self) -> None: ...

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[ValueT]: ...
