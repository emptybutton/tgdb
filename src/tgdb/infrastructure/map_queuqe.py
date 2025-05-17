from collections import OrderedDict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol, Self, overload


class MapQueuqeKey(Protocol):
    def __le__(self, other: Self, /) -> bool: ...

    def __ge__(self, other: Self, /) -> bool: ...

    def __lt__(self, other: Self, /) -> bool: ...

    def __gt__(self, other: Self, /) -> bool: ...


@dataclass
class MapQueuqe[KeyT: int, ValueT](Mapping[KeyT, ValueT]):
    _map: OrderedDict[KeyT, ValueT] = field(
        default_factory=OrderedDict, init=False
    )

    def push(self, key: KeyT, value: ValueT) -> None:
        max_key = max(self, default=None)

        if max_key and key <= max_key:
            raise ValueError

        self._map[key] = value

    @overload
    def pull(self, count: None = None) -> tuple[KeyT, ValueT]: ...

    @overload
    def pull(self, count: int) -> tuple[tuple[KeyT, ValueT], ...]: ...

    def pull(
        self, count: int | None = None
    ) -> tuple[KeyT, ValueT] | tuple[tuple[KeyT, ValueT], ...]:
        if count is None:
            return self._map.popitem(last=False)

        return tuple(
            self._map.popitem(last=False)
            for _ in range(count)
        )

    def __getitem__(self, key: KeyT) -> ValueT:
        return self._map[key]

    def __iter__(self) -> Iterator[KeyT]:
        return iter(self._map)

    def __len__(self) -> int:
        return len(self._map)
