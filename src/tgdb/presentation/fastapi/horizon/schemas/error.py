from typing import Literal

from pydantic import BaseModel, Field

from tgdb.entities.horizon.transaction import ConflictError
from tgdb.presentation.fastapi.horizon.schemas.claim import ClaimSchema


class TransactionConflictSchema(BaseModel):
    """
    Transaction conflicts with another transaction.
    """

    type: Literal["transactionConflict"] = "transactionConflict"
    rejected_claims: tuple[ClaimSchema, ...] = Field(alias="rejectedClaims")

    @classmethod
    def of(cls, conflict: ConflictError) -> "TransactionConflictSchema":
        return TransactionConflictSchema(
            rejectedClaims=tuple(map(ClaimSchema.of, conflict.rejected_claims)),
        )


class NoTransactionSchema(BaseModel):
    """
    Transaction did not exist initially or was rolled back automatically.
    """

    type: Literal["noTransaction"] = "noTransaction"


class TransactionCommittingSchema(BaseModel):
    """
    Transaction is in the process of being committed, so it cannot be changed
    at this time.
    """

    type: Literal["transactionCommitting"] = "transactionCommitting"
