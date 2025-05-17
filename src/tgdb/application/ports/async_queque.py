from abc import ABC, abstractmethod
from collections.abc import AsyncIterator


class AsyncQueque[ValueT](ABC):
    @abstractmethod
    async def push(self, value: ValueT) -> None: ...

    @abstractmethod
    async def iter(self) -> AsyncIterator[ValueT]: ...
