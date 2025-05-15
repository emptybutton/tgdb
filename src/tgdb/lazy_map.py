from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field


@dataclass(frozen=True, unsafe_hash=False)
class LazyMap[KeyT, ValueT]:
    value_by_key: Callable[[KeyT], Awaitable[ValueT | None]]
    _computed_map: dict[KeyT, ValueT] = field(init=False, default_factory=dict)

    async def __getitem__(self, key: KeyT) -> ValueT | None:
        with suppress(KeyError):
            return self._computed_map[key]

        value = await self.value_by_key(key)

        if value is None:
            return None

        self._computed_map[key] = value

        return value

    def __setitem__(self, key: KeyT, value: ValueT) -> None:
        self._computed_map[key] = value

    def __delitem__(self, key: KeyT) -> None:
        del self._computed_map[key]
