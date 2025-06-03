from collections.abc import Iterable
from typing import Annotated

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Query, status
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, PositiveInt

from tgdb.application.view_tuples import ViewTuples
from tgdb.entities.horizon.transaction import XID
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import Tuple
from tgdb.presentation.fastapi.common.schemas.error import NoRelationSchema
from tgdb.presentation.fastapi.common.tags import Tag
from tgdb.presentation.fastapi.horizon.schemas.error import (
    InvalidTransactionStateSchema,
    NoTransactionSchema,
)
from tgdb.presentation.fastapi.relation.schemas.tuple import TupleSchema


view_tuples_router = APIRouter()


class ViewedTuplesSchema(BaseModel):
    tuples: tuple[TupleSchema, ...]

    @classmethod
    def of(cls, tuples: Iterable[Tuple]) -> "ViewedTuplesSchema":
        tuple_schemas = tuple(map(TupleSchema.of, tuples))

        return ViewedTuplesSchema(tuples=tuple_schemas)


@view_tuples_router.post(
    "/transactions/{xid}/viewed-tuples",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_200_OK: {"model": ViewedTuplesSchema},
        status.HTTP_400_BAD_REQUEST: {"model": InvalidTransactionStateSchema},
        status.HTTP_404_NOT_FOUND: {
            "model": NoRelationSchema | NoTransactionSchema
        },
    },
    summary="View tuples",
    description="View tuples in an active transaction.",
    tags=[Tag.relation, Tag.transaction],
)
@inject
async def _(
    view_tuples: FromDishka[ViewTuples],
    xid: XID,
    relation_number: Annotated[PositiveInt, Query(alias="relationNumber")],
    attribute_number: Annotated[PositiveInt, Query(alias="attributeNumber")],
    attribute_scalar: Annotated[Scalar, Query(alias="attributeScalar")],
) -> Response:
    tuples = await view_tuples(
        xid,
        Number(relation_number),
        Number(attribute_number),
        attribute_scalar,
    )

    response_body_model = ViewedTuplesSchema.of(tuples)
    response_body = response_body_model.model_dump(mode="json", by_alias=True)

    return JSONResponse(response_body, status_code=status.HTTP_200_OK)
