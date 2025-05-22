from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import Response

from tgdb.application.input_operator import InputOperator
from tgdb.presentation.fastapi.schemas.entity import RollbackOperatorSchema
from tgdb.presentation.fastapi.tags import Tag


rollback_transaction_router = APIRouter()


@rollback_transaction_router.delete(
    "/transactions",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={status.HTTP_204_NO_CONTENT: {"content": None}},
    summary="Rollback transaction",
    description=(
        "If you don't roll back a transaction"
        ", it will roll back itself over time."
    ),
    tags=[Tag.transaction],
)
@inject
async def _(
    input_operator: FromDishka[InputOperator[RollbackOperatorSchema]],
    request_body: RollbackOperatorSchema,
) -> Response:
    await input_operator(request_body)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
