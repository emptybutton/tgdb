from dataclasses import dataclass, field

from tgdb.presentation.async_result import AsyncResult


@dataclass(frozen=True, unsafe_hash=False)
class AsyncMap[KeyT, ValueT]:
    _map: dict[KeyT, AsyncResult[ValueT]] = field(
        init=False, default_factory=dict
    )

    def __getitem__(self, key: KeyT) -> AsyncResult[ValueT]:
        if key in self._map:
            return self._map[key]

        result = AsyncResult[ValueT]()
        self._map[key] = result

        return result

    def __setitem__(self, key: KeyT, value: ValueT) -> None:
        if key not in self._map:
            return

        result = self._map[key]
        result.set(value)
