from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse

from tgdb.application.common.ports.relations import NoRelationError
from tgdb.presentation.fastapi.common.schemas.error import NoRelationSchema
from tgdb.presentation.fastapi.horizon.error_handling import (
    add_horizon_error_handling,
)


def add_error_handling(app: FastAPI) -> None:
    add_common_error_handling(app)
    add_horizon_error_handling(app)


def add_common_error_handling(app: FastAPI) -> None:
    @app.exception_handler(NoRelationError)
    def _(_: object) -> Response:
        schema = NoRelationSchema()

        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_404_NOT_FOUND,
        )
