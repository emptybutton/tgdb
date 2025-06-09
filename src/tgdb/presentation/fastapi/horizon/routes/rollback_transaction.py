from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import Response

from tgdb.application.horizon.rollback_transaction import RollbackTransaction
from tgdb.entities.horizon.transaction import XID
from tgdb.presentation.fastapi.common.tags import Tag
from tgdb.presentation.fastapi.horizon.schemas.error import (
    NoTransactionSchema,
    TransactionCommittingSchema,
)


rollback_transaction_router = APIRouter()


@rollback_transaction_router.delete(
    "/transactions/{xid}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_204_NO_CONTENT: {"content": None},
        status.HTTP_400_BAD_REQUEST: {"model": TransactionCommittingSchema},
        status.HTTP_404_NOT_FOUND: {"model": NoTransactionSchema},
    },
    summary="Rollback transaction",
    description="Rollback a transaction as if it had never started.",
    tags=[Tag.transaction],
)
@inject
async def _(
    rollback_transaction: FromDishka[RollbackTransaction],
    xid: XID,
) -> Response:
    await rollback_transaction(xid)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
