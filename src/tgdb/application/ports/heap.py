from abc import ABC, abstractmethod

from tgdb.entities.transaction import TransactionEffect


class Heap(ABC):
    @abstractmethod
    async def map(self, effect: TransactionEffect, /) -> None: ...
