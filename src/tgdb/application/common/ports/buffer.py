from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, Sequence


class Buffer[ValueT](ABC, AsyncIterable[Sequence[ValueT]]):
    @abstractmethod
    async def add(self, value: ValueT, /) -> None: ...
