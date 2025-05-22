from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import Response

from tgdb.application.input_operator import InputOperator
from tgdb.presentation.fastapi.schemas.entity import StartOperatorSchema
from tgdb.presentation.fastapi.tags import Tag


start_transaction_router = APIRouter()


@start_transaction_router.post(
    "/transactions",
    status_code=status.HTTP_201_CREATED,
    responses={status.HTTP_201_CREATED: {"content": None}},
    summary="Start transaction",
    description=(
        "Start transaction. Only after successful completion can you start"
        " reading, even if non-serializable reading is used."
    ),
    tags=[Tag.transaction],
)
@inject
async def _(
    input_operator: FromDishka[InputOperator[StartOperatorSchema]],
    request_body: StartOperatorSchema,
) -> Response:
    await input_operator(request_body)

    return Response(status_code=status.HTTP_201_CREATED)
