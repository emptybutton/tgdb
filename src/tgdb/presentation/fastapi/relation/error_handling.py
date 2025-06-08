from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse

from tgdb.application.relation.ports.relations import (
    NoRelationError,
    NotUniqueRelationNumberError,
)
from tgdb.application.relation.ports.tuples import OversizedRelationSchemaError
from tgdb.entities.relation.tuple_effect import InvalidRelationTupleError
from tgdb.presentation.fastapi.relation.schemas.error import (
    InvalidRelationTupleSchema,
    NoRelationSchema,
    NotUniqueRelationNumberSchema,
    OversizedRelationSchemaSchema,
)


def add_relation_error_handling(app: FastAPI) -> None:
    @app.exception_handler(NoRelationError)
    def _(_: object, __: object) -> Response:
        schema = NoRelationSchema()

        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_404_NOT_FOUND,
        )

    @app.exception_handler(NotUniqueRelationNumberError)
    def _(_: object, __: object) -> Response:
        schema = NotUniqueRelationNumberSchema()

        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_409_CONFLICT,
        )

    @app.exception_handler(OversizedRelationSchemaError)
    def _(_: object, error: OversizedRelationSchemaError) -> Response:
        schema = OversizedRelationSchemaSchema.of(error)

        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    @app.exception_handler(InvalidRelationTupleError)
    def _(_: object, error: InvalidRelationTupleError) -> Response:
        schema = InvalidRelationTupleSchema.of(error)

        return JSONResponse(
            schema.model_dump(mode="json", by_alias=True),
            status_code=status.HTTP_400_BAD_REQUEST,
        )
