from dataclasses import dataclass, field

from tgdb.application.ports.heap import Heap
from tgdb.entities.operator import DeletedRow, MutatedRow, NewRow
from tgdb.entities.row import Row
from tgdb.entities.transaction import TransactionEffect


@dataclass(frozen=True, unsafe_hash=False)
class InMemoryHeap(Heap):
    _rows: set[Row] = field(default_factory=set)

    async def map(self, effect: TransactionEffect) -> None:
        for row_operator_effect in effect:
            match row_operator_effect, row_operator_effect.row in self._rows:
                case NewRow(row) | MutatedRow(row), _:
                    self._rows.add(row)
                case DeletedRow(row), True:
                    self._rows.remove(row)
                case _: ...
