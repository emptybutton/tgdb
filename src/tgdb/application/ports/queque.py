from abc import ABC, abstractmethod
from collections.abc import AsyncIterator


class Queque[ValueT](ABC):
    @abstractmethod
    async def push(self, value: ValueT) -> None: ...

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[ValueT]: ...
