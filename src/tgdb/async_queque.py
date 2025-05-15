from asyncio import Event
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field


@dataclass(frozen=True, unsafe_hash=False)
class AsyncQueque[ValueT]:
    _values: deque[ValueT] = field(default_factory=deque)
    _offset_by_event: dict[Event, int] = field(
        default_factory=dict, init=False
    )

    def __len__(self) -> int:
        return len(self._values)

    def push(self, value: ValueT) -> None:
        self._values.append(value)

        for event in self._offset_by_event:
            event.set()

    async def __aiter__(self) -> AsyncIterator[ValueT]:
        event = Event()
        self._offset_by_event[event] = -1

        while True:
            await event.wait()

            self._offset_by_event[event] += 1
            new_value = self._values[self._offset_by_event[event]]

            self._refresh()

            yield new_value

    def _refresh(self) -> None:
        min_offset = min(self._offset_by_event.values())

        if min_offset >= 0:
            self._values.popleft()

            for event in self._offset_by_event:
                self._offset_by_event[event] -= 1
