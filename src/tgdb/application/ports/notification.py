from abc import ABC, abstractmethod


class Notification[ValueT](ABC):
    @abstractmethod
    async def send(self, value: ValueT, /) -> None: ...
