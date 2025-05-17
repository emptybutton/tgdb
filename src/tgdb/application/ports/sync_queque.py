from abc import ABC, abstractmethod
from collections.abc import AsyncIterator


class SyncQueque[ValueT](ABC):
    @abstractmethod
    async def async_push(self, value: ValueT) -> None: ...

    @abstractmethod
    async def sync_push(self, value: ValueT) -> None: ...

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[ValueT]: ...
