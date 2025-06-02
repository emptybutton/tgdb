from typing import Literal

from pydantic import BaseModel, Field

from tgdb.entities.horizon.transaction import ConflictError
from tgdb.presentation.fastapi.schemas.claim import ClaimSchema


class TransactionConflictSchema(BaseModel):
    """
    Transaction conflicts with another transaction.
    """

    type: Literal["transactionConflict"] = "transactionConflict"
    rejected_claims: tuple[ClaimSchema, ...] = Field(alias="rejectedClaims")

    @classmethod
    def of(cls, conflict: ConflictError) -> "TransactionConflictSchema":
        return TransactionConflictSchema(
            rejectedClaims=tuple(map(ClaimSchema.of, conflict.rejected_claims))
        )


class NoTransactionSchema(BaseModel):
    """
    Transaction did not exist initially or was rolled back automatically.
    """

    type: Literal["noTransaction"] = "noTransaction"


class InvalidTransactionStateSchema(BaseModel):
    """
    The transaction exists, but the action cannot be applied because the order
    of operations is incorrect.
    """

    type: Literal["invalidTransactionState"] = "invalidTransactionState"


class NonSerializableWriteTransactioneSchema(BaseModel):
    """
    Transaction write could not be serialized.
    """

    type: Literal["nonSerializableWriteTransaction"] = (
        "nonSerializableWriteTransaction"
    )
