from abc import ABC, abstractmethod
from collections.abc import AsyncIterable


class Queque[ValueT](ABC, AsyncIterable[ValueT]):
    @abstractmethod
    async def push(self, *values: ValueT) -> None: ...

    @abstractmethod
    async def sync(self) -> None: ...
