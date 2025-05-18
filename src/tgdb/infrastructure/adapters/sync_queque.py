from asyncio import Event
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from tgdb.application.ports.sync_queque import SyncQueque


@dataclass
class InMemorySyncQueque[ValueT](SyncQueque[ValueT]):
    _values: deque[ValueT]
    _push_completion_event: Event = field(init=False, default_factory=Event)
    _pull_activation_events: list[Event] = field(
        init=False, default_factory=list
    )

    async def async_push(self, value: ValueT) -> None:
        self._values.append(value)

    async def sync_push(self, value: ValueT) -> None:
        self._values.append(value)

        self._push_completion_event.clear()
        await self._push_completion_event.wait()

        self._values.clear()

    async def __aiter__(self) -> AsyncIterator[ValueT]:
        is_pull_active = Event()
        self._pull_activation_events.append(is_pull_active)

        if self._values:
            is_pull_active.set()

        while True:
            await is_pull_active.wait()

            for value in self._values:
                yield value

            is_pull_active.clear()

            self._update_push_completion_event()

    def _update_push_completion_event(self) -> None:
        if self._no_active_pulls():
            self._push_completion_event.set()

    def _no_active_pulls(self) -> bool:
        return all(
            not pull_activation_event.is_set()
            for pull_activation_event in self._pull_activation_events
        )
