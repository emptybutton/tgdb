from asyncio import Event
from collections import deque
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field


@dataclass(frozen=True, unsafe_hash=False)
class AsyncQueque[ValueT]:
    _values: deque[ValueT] = field(default_factory=deque)
    _is_synced: Event = field(default_factory=Event, init=False)
    _offset_by_iteration_event: dict[Event, int] = field(
        default_factory=dict,
        init=False,
    )

    def __post_init__(self) -> None:
        if not self._values:
            self._is_synced.set()

    def __len__(self) -> int:
        return len(self._values)

    def __bool__(self) -> bool:
        return bool(self._values)

    def __iter__(self) -> Iterator[ValueT]:
        return iter(self._values)

    def __getitem__(self, index: int, /) -> ValueT:
        return self._values[index]

    def push(self, value: ValueT) -> None:
        self._values.append(value)

        self._is_synced.clear()

        for event in self._offset_by_iteration_event:
            event.set()

    def __aiter__(self) -> AsyncIterator[ValueT]:
        is_iteration_active = Event()

        if self._values:
            is_iteration_active.set()

        self._offset_by_iteration_event[is_iteration_active] = -1

        return self._iteration(is_iteration_active)

    async def sync(self) -> None:
        await self._is_synced.wait()

    async def _iteration(self, is_active: Event) -> AsyncIterator[ValueT]:
        while True:
            await is_active.wait()

            self._offset_by_iteration_event[is_active] += 1
            new_value = self._values[self._offset_by_iteration_event[is_active]]

            self._refresh()

            try:
                yield new_value
            except Exception as error:
                del self._offset_by_iteration_event[is_active]
                raise error from error

            was_end_commited = (
                self._offset_by_iteration_event[is_active]
                == len(self._values) - 1
            )

            if was_end_commited:
                is_active.clear()

            if not self._values:
                self._is_synced.set()

    def _refresh(self) -> None:
        min_offset = min(self._offset_by_iteration_event.values(), default=-1)

        if min_offset >= 0:
            self._values.popleft()

            for event in self._offset_by_iteration_event:
                self._offset_by_iteration_event[event] -= 1
