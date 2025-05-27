from uuid import UUID

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from tgdb.application.input_operator import InputOperator
from tgdb.entities.transaction import (
    NoTransaction,
    TransactionCommit,
    TransactionConflict,
    TransactionCommit,
)
from tgdb.presentation.async_map import AsyncMap
from tgdb.presentation.fastapi.schemas.entity import StartOperatorSchema
from tgdb.presentation.fastapi.schemas.output import (
    NoTransactionSchema,
    TransactionConflictSchema,
)
from tgdb.presentation.fastapi.tags import Tag


commit_transaction_router = APIRouter()


description = """
Write down all a transaction statements and commit it.

Transactions can be automatically rolled back if:
1. They violate serializability
2. The server crashed
3. The server ran out of space for active transactions

Therefore, in case of any negative response, you should retry transactions in full, and not just send a new commit.
"""  # noqa: E501


@commit_transaction_router.post(
    "/transactions/commit",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_204_NO_CONTENT: {"content": None},
        status.HTTP_404_NOT_FOUND: {"model": NoTransactionSchema},
        status.HTTP_409_CONFLICT: {"content": TransactionConflictSchema},
    },
    summary="Commit transaction",
    description=description,
    tags=[Tag.transaction],
)
@inject
async def _(
    input_operator: FromDishka[InputOperator[StartOperatorSchema]],
    commit_map: FromDishka[AsyncMap[UUID, TransactionCommit]],
    request_body: StartOperatorSchema,
) -> Response:
    async_commit = commit_map[request_body.xid]

    await input_operator(request_body)
    commit = await async_commit

    if isinstance(commit, TransactionCommit):
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    match commit.reason:
        case NoTransaction():
            response_body_schema: BaseModel = NoTransactionSchema()
            response_body = response_body_schema.model_dump(
                mode="json", by_alias=True
            )

            return JSONResponse(
                response_body, status_code=status.HTTP_404_NOT_FOUND
            )
        case TransactionConflict() as conflict:
            response_body_schema = TransactionConflictSchema.of(conflict)
            response_body = response_body_schema.model_dump(
                mode="json", by_alias=True
            )

            return JSONResponse(
                response_body, status_code=status.HTTP_409_CONFLICT
            )
