from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse

from tgdb.entities.horizon.horizon import (
    NoTransactionError,
    TransactionCommittingError,
)
from tgdb.entities.horizon.transaction import ConflictError
from tgdb.presentation.fastapi.horizon.schemas.error import (
    NoTransactionSchema,
    TransactionCommittingSchema,
    TransactionConflictSchema,
)


def add_horizon_error_handling(app: FastAPI) -> None:
    @app.exception_handler(ConflictError)
    def _(_: object, conflict: ConflictError) -> Response:
        schema = TransactionConflictSchema.of(conflict)

        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_409_CONFLICT,
        )

    @app.exception_handler(NoTransactionError)
    def _(_: object, __: object) -> Response:
        return JSONResponse(
            NoTransactionSchema().model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_404_NOT_FOUND,
        )

    @app.exception_handler(TransactionCommittingError)
    def _(_: object, __: object) -> Response:
        schema = TransactionCommittingSchema()
        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_400_BAD_REQUEST,
        )
