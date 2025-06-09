from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import ClassVar


class NoExternalValue:
    _instance: "ClassVar[NoExternalValue | None]" = None

    def __new__(cls) -> "NoExternalValue":
        if NoExternalValue._instance is not None:
            return NoExternalValue._instance

        instance = super().__new__(cls)
        NoExternalValue._instance = instance

        return instance


type ExternalValue[ValueT] = ValueT | NoExternalValue


@dataclass(frozen=True, unsafe_hash=False)
class LazyMap[KeyT, ValueT]:
    _cache_map_max_len: int
    _external_value: Callable[[KeyT], Awaitable[ExternalValue[ValueT]]]

    _cache_map: OrderedDict[KeyT, ExternalValue[ValueT]] = field(
        init=False,
        default_factory=OrderedDict,
    )

    def cache_map(self) -> OrderedDict[KeyT, ExternalValue[ValueT]]:
        return OrderedDict(self._cache_map)

    async def __getitem__(self, key: KeyT) -> ValueT:
        if key in self._cache_map:
            return self._output(self._cache_map[key])

        value = await self._external_value(key)
        self._insert_to_cache_map(key, value)

        return self._output(value)

    def __setitem__(self, key: KeyT, value: ValueT) -> None:
        self._insert_to_cache_map(key, value)

    def _output(self, value: ExternalValue[ValueT]) -> ValueT:
        if isinstance(value, NoExternalValue):
            raise KeyError

        return value

    def _insert_to_cache_map(
        self,
        key: KeyT,
        value: ExternalValue[ValueT],
    ) -> None:
        self._cache_map[key] = value

        if len(self._cache_map) > self._cache_map_max_len:
            self._cache_map.pop(next(iter(self._cache_map)))
