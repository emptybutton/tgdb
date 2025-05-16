from abc import ABC, abstractmethod
from asyncio import Event, Semaphore
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from tgdb.application.ports.queque import Queque
from tgdb.infrastructure.async_queque import AsyncQueque


@dataclass
class InMemoryQueque[ValueT](Queque[ValueT]):
    _queque: AsyncQueque[ValueT]
    _sync_reading_completed: Event = field(init=False, default_factory=Event)

    async def async_push(self, value: ValueT) -> None:
        self._queque.push(value)

    async def sync_push(self, value: ValueT) -> None:
        self._sync_reading_completed.clear()
        self._queque.push(value)
        await self._sync_reading_completed.wait()

    async def async_(self) -> AsyncIterator[ValueT]:
        values = self._async_values()
        first_value = await anext(values)

        return self._values(first_value, values)

    async def _values(
        self, first_value: ValueT, values: AsyncIterator[ValueT]
    ) -> AsyncIterator[ValueT]:
        yield first_value

        async for value in values:
            yield value

    async def _async_values(self) -> AsyncIterator[ValueT]:
        async for value in self._queque:
            yield value

    async def sync(self) -> AsyncIterator[ValueT]:
        for value in 
