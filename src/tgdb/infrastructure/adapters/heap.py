from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from functools import partial

from tgdb.application.ports.heap import Heap
from tgdb.entities.row import Row, RowAttribute, RowSchema
from tgdb.entities.transaction import TransactionEffect


@dataclass(frozen=True, unsafe_hash=False)
class InMemoryHeap(Heap):
    _row_by_row_id: dict[RowAttribute, Row] = field(default_factory=dict)

    async def insert(self, row: Row) -> None:
        if row.id in self._row_by_row_id:
            raise ValueError

        self._row_by_row_id[row.id] = row

    async def row(
        self,
        schema: RowSchema,
        attribute_number: int,
        attribute: RowAttribute | None = None,
    ) -> Row | None:
        for row in self._row_by_row_id.values():
            if self._is_valid(schema, attribute_number, attribute, row):
                return row

        return None

    async def rows(
        self,
        schema: RowSchema,
        attribute_number: int,
        attribute: RowAttribute | None = None,
    ) -> AsyncIterator[Row]:
        is_valid = partial(self._is_valid, schema, attribute_number, attribute)
        rows = tuple(filter(is_valid, self._row_by_row_id.values()))

        for row in rows:
            yield row

    async def update(self, row: Row) -> None:
        if row.id not in self._row_by_row_id:
            raise ValueError

        self._row_by_row_id[row.id] = row

    async def delete(self, row: Row) -> None:
        del self._row_by_row_id[row.id]

    async def map(self, effect: TransactionEffect) -> None:
        for new_row in effect.new_values:
            await self.insert(new_row)

        for mutated_row in effect.mutated_values:
            await self.update(mutated_row)

        for dead_row in effect.dead_values:
            await self.delete(dead_row)

    def _is_valid(
        self,
        schema: RowSchema,
        attribute_number: int,
        attribute: RowAttribute | None,
        row: Row,
    ) -> bool:
        return (
            row.schema == schema
            and len(row) >= attribute_number + 1
            and (
                attribute is not None
                or row[attribute_number] == attribute
            )
        )
