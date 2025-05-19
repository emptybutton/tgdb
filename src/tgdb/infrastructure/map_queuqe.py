from collections import OrderedDict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol, Self, overload

from tgdb.entities.assert_ import not_none


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
    _min: int | None = field(default=None, init=False)
    _max: int | None = field(default=None, init=False)

    def min(self) -> int | None:
        return self._min

    def max(self) -> int | None:
        return self._max

    def push(self, key: int, value: ValueT) -> None:
        if self._max is not None and key <= self._max:
            raise ValueError

        if self._min is None:
            self._min = key

        self._max = key
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

        pulled_pairs = tuple(
            self._map.popitem(last=False)
            for _ in range(count)
        )

        if not self._map:
            self._min = None
            self._max = None

        self._min = next(iter(self._map))

        return pulled_pairs

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
            return self._min

        if key >= not_none(self._max):
            return None

        next_key = key + 1

        while next_key not in self._map:
            next_key += 1

        return next_key
