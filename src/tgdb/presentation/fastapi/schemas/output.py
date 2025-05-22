from typing import Literal

from pydantic import BaseModel

from tgdb.entities.transaction import TransactionConflict
from tgdb.presentation.fastapi.schemas.entity import MarkOperatorSchema


class TransactionConflictSchema(BaseModel):
    type: Literal["transactionConflict"] = "transactionConflict"
    marks: tuple[MarkOperatorSchema, ...]

    @classmethod
    def of(cls, conflict: TransactionConflict) -> "TransactionConflictSchema":
        return TransactionConflictSchema(
            marks=tuple(map(MarkOperatorSchema.of, conflict.marks))
        )


class NoTransactionSchema(BaseModel):
    type: Literal["noTransaction"] = "noTransaction"
