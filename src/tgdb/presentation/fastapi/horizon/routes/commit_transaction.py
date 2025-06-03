from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import Response
from pydantic import BaseModel

from tgdb.application.common.operator import Operator
from tgdb.application.horizon.commit_transaction import CommitTransaction
from tgdb.entities.horizon.transaction import (
    XID,
    NonSerializableWriteTransactionError,
)
from tgdb.presentation.fastapi.common.tags import Tag
from tgdb.presentation.fastapi.horizon.schemas.error import (
    InvalidTransactionStateSchema,
    NoTransactionSchema,
    TransactionConflictSchema,
)


commit_transaction_router = APIRouter()

description = """
Write down all a transaction statements and commit it.

Transactions can be automatically rolled back if:
1. They violate serializability
2. The server crashed
3. The server ran out of space for active transactions

Therefore, in case of any negative response, you should retry transactions in full, and not just send a new commit.
"""  # noqa: E501


class CommitTransactionSchema(BaseModel):
    operators: tuple[Operator, ...]


@commit_transaction_router.post(
    "/transactions/{xid}/commit",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_204_NO_CONTENT: {"content": None},
        status.HTTP_404_NOT_FOUND: {"model": NoTransactionSchema},
        status.HTTP_400_BAD_REQUEST: {
            "model": (
                InvalidTransactionStateSchema
                | NonSerializableWriteTransactionError
            )
        },
        status.HTTP_409_CONFLICT: {"content": TransactionConflictSchema},
    },
    summary="Commit transaction",
    description=description,
    tags=[Tag.transaction],
)
@inject
async def _(
    commit_transaction: FromDishka[CommitTransaction],
    xid: XID,
    request_body: CommitTransactionSchema,
) -> Response:
    await commit_transaction(xid, request_body.operators)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
