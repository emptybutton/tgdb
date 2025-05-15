from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from tgdb.entities.row import RowAttribute


class TransactionState(StrEnum):
    started = "started"
    committed = "committed"
    rollbacked = "rollbacked"


@dataclass(frozen=True)
class TransactionStateMark:
    transaction_state: TransactionState


@dataclass(frozen=True)
class TransactionUniquenessMark:
    mark_id: UUID
    key: str


@dataclass(frozen=True)
class TransactionViewedRowMark:
    row_id: RowAttribute


type TransactionMark = (
    TransactionStateMark
    | TransactionUniquenessMark
    | TransactionViewedRowMark
)
