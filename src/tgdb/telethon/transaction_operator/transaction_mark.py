from enum import StrEnum
from uuid import UUID

from tgdb.telethon.row import RowSchema


transaction_uniqueness_mark_schema = RowSchema(
    "__transaction_uniqueness__", UUID, (str, UUID)
)


class TransactionState(StrEnum):
    started = "started"
    committed = "committed"
    rollbacked = "rollbacked"


transaction_state_mark_schema = RowSchema(
    "__transaction_result__",
    UUID,
    (TransactionState, ),
)


viewed_row_mark_schema = RowSchema(
    "__viewed_row__", UUID, (UUID, ),
)
