from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from tgdb.application.horizon.start_transaction import StartTransaction
from tgdb.entities.horizon.transaction import XID
from tgdb.presentation.fastapi.common.tags import Tag
from tgdb.presentation.fastapi.horizon.schemas.isolation_level import (
    IsolationLevelSchema,
)


start_transaction_router = APIRouter()


class StartTransactionSchema(BaseModel):
    isolation_level: IsolationLevelSchema = Field(alias="isolationLevel")


class StartedTransactionSchema(BaseModel):
    xid: XID


@start_transaction_router.post(
    "/transactions",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_201_CREATED: {"model": StartedTransactionSchema},
    },
    summary="Start transaction",
    description="Start new transaction.",
    tags=[Tag.transaction],
)
@inject
async def _(
    start_transaction: FromDishka[StartTransaction],
    request_body: StartTransactionSchema,
) -> Response:
    xid = await start_transaction(request_body.isolation_level.decoded())

    response_body_model = StartedTransactionSchema(xid=xid)
    response_body = response_body_model.model_dump(mode="json", by_alias=True)

    return JSONResponse(response_body, status_code=status.HTTP_201_CREATED)
