from abc import ABC, abstractmethod

from tgdb.entities.row import Row
from tgdb.entities.transaction import TransactionEffect


class Heap(ABC):
    @abstractmethod
    async def insert(self, row: Row) -> None: ...

    @abstractmethod
    async def update(self, row: Row) -> None: ...

    @abstractmethod
    async def delete(self, row: Row) -> None: ...

    @abstractmethod
    async def map(self, effect: TransactionEffect) -> None: ...
