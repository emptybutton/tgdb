from asyncio import Event
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field

from tgdb.application.ports.async_queque import AsyncQueque
from tgdb.infrastructure.concurrent_queque import ConcurrentQueque


@dataclass
class InMemoryAsyncQueque[ValueT](AsyncQueque[ValueT]):
    _queque: ConcurrentQueque[ValueT]
    _sync_reading_completed: Event = field(init=False, default_factory=Event)

    async def push(self, *values: ValueT) -> None:
        for value in values:
            self._queque.push(value)

    async def iter(self) -> AsyncIterator[ValueT]:
        iter_ = aiter(self._queque)
        first_value = await anext(iter_)

        return self._compound_iter(first_value, iter_)

    async def _compound_iter(
        self, first_value: ValueT, other_values: AsyncIterable[ValueT]
    ) -> AsyncIterator[ValueT]:
        yield first_value

        async for other_value in other_values:
            yield other_value
