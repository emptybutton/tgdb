from collections.abc import AsyncIterator
from dataclasses import dataclass

from tgdb.application.common.ports.queque import Queque
from tgdb.infrastructure.async_queque import AsyncQueque


@dataclass
class InMemoryQueque[ValueT](Queque[ValueT]):
    _queque: AsyncQueque[ValueT]

    async def push(self, *values: ValueT) -> None:
        for value in values:
            self._queque.push(value)

    async def sync(self) -> None:
        await self._queque.sync()

    def __aiter__(self) -> AsyncIterator[ValueT]:
        return aiter(self._queque)
