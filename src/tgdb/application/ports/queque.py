from abc import ABC, abstractmethod
from collections.abc import AsyncIterator


class Queque[ValueT](ABC):
    @abstractmethod
    async def async_push(self, value: ValueT) -> None: ...

    @abstractmethod
    async def sync_push(self, value: ValueT) -> None: ...

    @abstractmethod
    async def async_(self) -> AsyncIterator[ValueT]: ...

    @abstractmethod
    def sync(self) -> AsyncIterator[ValueT]: ...
