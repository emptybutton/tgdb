from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import Response
from pydantic import BaseModel

from tgdb.application.horizon.commit_transaction import CommitTransaction
from tgdb.entities.horizon.transaction import (
    XID,
)
from tgdb.presentation.fastapi.common.schemas.operator import OperatorSchema
from tgdb.presentation.fastapi.common.tags import Tag
from tgdb.presentation.fastapi.horizon.schemas.error import (
    NoTransactionSchema,
    TransactionCommittingSchema,
    TransactionConflictSchema,
)
from tgdb.presentation.fastapi.relation.schemas.error import (
    InvalidRelationTupleSchema,
)


commit_transaction_router = APIRouter()

description = """
Record all transaction operators and commit them.

A transaction may fail to commit in the following cases:
1. It is a serializable transaction that conflicts with another serializable transaction.
2. The server runs out of space for active transactions and rolls back your transaction.
3. The transaction rolled back due to timeout.
4. The server crashes.

If the commit fails, all changes in the transaction will not be applied.
Therefore, for any negative response, retry the entire transaction.
"""  # noqa: E501


class CommitTransactionSchema(BaseModel):
    operators: tuple[OperatorSchema, ...]


@commit_transaction_router.post(
    "/transactions/{xid}/commit",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_204_NO_CONTENT: {"content": None},
        status.HTTP_404_NOT_FOUND: {"model": NoTransactionSchema},
        status.HTTP_400_BAD_REQUEST: {
            "model": InvalidRelationTupleSchema | TransactionCommittingSchema
        },
        status.HTTP_409_CONFLICT: {"model": TransactionConflictSchema},
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
    operators = tuple(operator.decoded() for operator in request_body.operators)
    await commit_transaction(xid, operators)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
