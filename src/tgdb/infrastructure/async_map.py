from asyncio import Future
from collections.abc import Awaitable, Iterator
from dataclasses import dataclass, field


@dataclass(frozen=True, unsafe_hash=False)
class AsyncMap[KeyT, ValueT]:
    _map: dict[KeyT, Future[ValueT]] = field(init=False, default_factory=dict)

    def __iter__(self) -> Iterator[KeyT]:
        return iter(self._map)

    def __len__(self) -> int:
        return len(self._map)

    def __getitem__(self, key: KeyT) -> Awaitable[ValueT]:
        if key in self._map:
            return self._map[key]

        futute_result = Future[ValueT]()
        self._map[key] = futute_result

        return futute_result

    def __setitem__(self, key: KeyT, result: ValueT) -> None:
        if key not in self._map:
            futute_result = Future[ValueT]()
            futute_result.set_result(result)
            self._map[key] = futute_result
            return

        futute_result = self._map[key]
        futute_result.set_result(result)

    def __delitem__(self, key: KeyT) -> None:
        del self._map[key]
