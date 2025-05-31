from collections import OrderedDict
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field


@dataclass(frozen=True, unsafe_hash=False)
class LazyMap[KeyT, ValueT]:
    _computed_map_max_len: int
    _external_value: Callable[[KeyT], Awaitable[ValueT | None]]
    _computed_map: OrderedDict[KeyT, ValueT] = field(
        init=False, default_factory=OrderedDict
    )

    async def __getitem__(self, key: KeyT) -> ValueT | None:
        with suppress(KeyError):
            return self._computed_map[key]

        value = await self._external_value(key)

        if value is None:
            return None

        self._computed_map[key] = value

        if len(self._computed_map) > self._computed_map_max_len:
            self._computed_map.pop(next(iter(self._computed_map)))

        return value

    def __setitem__(self, key: KeyT, value: ValueT) -> None:
        self._computed_map[key] = value

    def __delitem__(self, key: KeyT) -> None:
        del self._computed_map[key]
