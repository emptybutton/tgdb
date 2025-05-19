from asyncio import Event
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from tgdb.application.ports.sync_queque import SyncQueque


@dataclass
class InMemorySyncQueque[ValueT](SyncQueque[ValueT]):
    _values: deque[ValueT]
    _is_synced: Event = field(init=False, default_factory=Event)
    _pull_activation_events: list[Event] = field(
        init=False, default_factory=list
    )

    async def push(self, *values: ValueT) -> None:
        self._values.extend(values)

        for is_pull_active in self._pull_activation_events:
            is_pull_active.set()

    async def sync(self) -> None:
        if not self._values:
            return

        self._is_synced.clear()
        await self._is_synced.wait()

    async def __aiter__(self) -> AsyncIterator[ValueT]:
        is_pull_active = Event()
        self._pull_activation_events.append(is_pull_active)

        if self._values:
            is_pull_active.set()

        while True:
            await is_pull_active.wait()
            is_pull_active.clear()

            for index in range(len(self._values)):
                yield self._values[index]

            if self._no_active_pulls():
                self._is_synced.set()

    def _no_active_pulls(self) -> bool:
        return all(
            not is_pull_active.is_set()
            for is_pull_active in self._pull_activation_events
        )
