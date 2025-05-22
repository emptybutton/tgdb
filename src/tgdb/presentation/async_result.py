from asyncio import Event
from collections.abc import Awaitable, Generator
from dataclasses import dataclass, field
from typing import Any, cast


@dataclass
class AsyncResult[ValueT](Awaitable[ValueT]):
    _value: ValueT | None = field(init=False, default=None)
    _is_value_set: Event = field(init=False, default_factory=Event)

    def set(self, value: ValueT) -> None:
        self._value = value
        self._is_value_set.set()

    def __await__(self) -> Generator[Any, Any, ValueT]:
        return self._get().__await__()

    async def _get(self) -> ValueT:
        await self._is_value_set.wait()
        return cast(ValueT, self._value)
