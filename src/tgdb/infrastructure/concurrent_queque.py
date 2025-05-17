from asyncio import Event
from collections import deque
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, overload


@dataclass(frozen=True, unsafe_hash=False)
class ConcurrentQueque[ValueT](Sequence[ValueT]):
    _values: deque[ValueT] = field(default_factory=deque)
    _offset_by_event: dict[Event, int] = field(
        default_factory=dict, init=False
    )

    def __len__(self) -> int:
        return len(self._values)

    def __bool__(self) -> bool:
        return bool(self._values)

    def __iter__(self) -> Iterator[ValueT]:
        return iter(self._values)

    @overload
    def __getitem__(self, index: int, /) -> ValueT: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[ValueT]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[ValueT] | ValueT:
        return self._values[value]

    def push(self, value: ValueT) -> None:
        self._values.append(value)

        for event in self._offset_by_event:
            event.set()

    async def __aiter__(self) -> AsyncIterator[ValueT]:
        event = Event()

        if self._values:
            event.set()

        self._offset_by_event[event] = -1

        while True:
            await event.wait()

            self._offset_by_event[event] += 1
            new_value = self._values[self._offset_by_event[event]]

            self._refresh()

            yield new_value

            if self._offset_by_event[event] == len(self._values) - 1:
                event.clear()

    def _refresh(self) -> None:
        min_offset = min(self._offset_by_event.values())

        if min_offset >= 0:
            self._values.popleft()

            for event in self._offset_by_event:
                self._offset_by_event[event] -= 1
