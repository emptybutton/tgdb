from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse

from tgdb.entities.horizon.horizon import (
    InvalidTransactionStateError,
    NoTransactionError,
)
from tgdb.entities.horizon.transaction import (
    ConflictError,
    NonSerializableWriteTransactionError,
)
from tgdb.presentation.fastapi.horizon.schemas.error import (
    InvalidTransactionStateSchema,
    NoTransactionSchema,
    TransactionConflictSchema,
)


def add_horizon_error_handling(app: FastAPI) -> None:
    @app.exception_handler(ConflictError)
    def _(conflict: ConflictError) -> Response:
        schema = TransactionConflictSchema.of(conflict)

        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_409_CONFLICT,
        )

    @app.exception_handler(NoTransactionError)
    def _(_: object) -> Response:
        return JSONResponse(
            NoTransactionSchema().model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_404_NOT_FOUND,
        )

    @app.exception_handler(InvalidTransactionStateError)
    def _(_: object) -> Response:
        schema = InvalidTransactionStateSchema()
        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_404_NOT_FOUND,
        )

    @app.exception_handler(NonSerializableWriteTransactionError)
    def _(_: object) -> Response:
        schema = InvalidTransactionStateSchema()
        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_404_NOT_FOUND,
        )
