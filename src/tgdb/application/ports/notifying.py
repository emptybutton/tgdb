from abc import ABC, abstractmethod


class Notifying[ValueT](ABC):
    @abstractmethod
    async def publish(self, value: ValueT, /) -> None: ...
