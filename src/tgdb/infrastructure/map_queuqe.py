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
class MapQueuqe[ValueT](Mapping[int, ValueT]):
    _map: OrderedDict[int, ValueT] = field(
        default_factory=OrderedDict, init=False
    )

    def push(self, key: int, value: ValueT) -> None:
        max_key = max(self, default=None)

        if max_key and key <= max_key:
            raise ValueError

        self._map[key] = value

    @overload
    def pull(self, count: None = None) -> tuple[int, ValueT]: ...

    @overload
    def pull(self, count: int) -> tuple[tuple[int, ValueT], ...]: ...

    def pull(
        self, count: int | None = None
    ) -> tuple[int, ValueT] | tuple[tuple[int, ValueT], ...]:
        if count is None:
            return self._map.popitem(last=False)

        return tuple(
            self._map.popitem(last=False)
            for _ in range(count)
        )

    def __getitem__(self, key: int) -> ValueT:
        return self._map[key]

    def __iter__(self) -> Iterator[int]:
        return iter(self._map)

    def __len__(self) -> int:
        return len(self._map)

    def next_key(self, key: int | None) -> int | None:
        if not self._map:
            return None

        if key is None:
            return min(self._map)

        if key >= max(self._map):
            return None

        next_key = key + 1

        while next_key not in self._map:
            next_key += 1

        return next_key
