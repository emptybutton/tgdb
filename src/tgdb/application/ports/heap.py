from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from tgdb.entities.row import Row, RowAttribute, RowSchema
from tgdb.entities.transaction import TransactionEffect


class Heap(ABC):
    @abstractmethod
    async def insert(self, row: Row) -> None: ...

    @abstractmethod
    async def row(
        self,
        schema: RowSchema,
        attribute_number: int,
        attribute: RowAttribute | None = None,
    ) -> Row | None: ...

    @abstractmethod
    def rows(
        self,
        schema: RowSchema,
        attribute_number: int,
        attribute: RowAttribute | None = None,
    ) -> AsyncIterator[Row]: ...

    @abstractmethod
    async def update(self, row: Row) -> None: ...

    @abstractmethod
    async def delete(self, row: Row) -> None: ...

    @abstractmethod
    async def map(self, effect: TransactionEffect) -> None: ...
