from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import Response

from tgdb.application.horizon.rollback_transaction import RollbackTransaction
from tgdb.entities.horizon.transaction import XID
from tgdb.presentation.fastapi.schemas.horizon.error import NoTransactionSchema
from tgdb.presentation.fastapi.tags import Tag


rollback_transaction_router = APIRouter()


@rollback_transaction_router.delete(
    "/transactions/{xid}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_204_NO_CONTENT: {"content": None},
        status.HTTP_404_NOT_FOUND: {"model": NoTransactionSchema},
    },
    summary="Rollback transaction",
    description=(
        "If you don't roll back a transaction"
        ", it will roll back itself over time."
    ),
    tags=[Tag.transaction],
)
@inject
async def _(
    rollback_transaction: FromDishka[RollbackTransaction],
    xid: XID,
) -> Response:
    await rollback_transaction(xid)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
