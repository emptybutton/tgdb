from typing import Annotated

from annotated_types import Ge
from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import Response
from pydantic import BaseModel, Field

from tgdb.application.relation.create_relation import CreateRelation
from tgdb.entities.numeration.number import Number
from tgdb.presentation.fastapi.common.tags import Tag
from tgdb.presentation.fastapi.relation.schemas.error import (
    NotUniqueRelationNumberSchema,
    OversizedRelationSchemaSchema,
)
from tgdb.presentation.fastapi.relation.schemas.schema import SchemaSchema


create_relation_router = APIRouter()


class CreateRelationSchema(BaseModel):
    schema_: SchemaSchema = Field(alias="schema")


@create_relation_router.post(
    "/relations/{relation_number}",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_201_CREATED: {"content": None},
        status.HTTP_400_BAD_REQUEST: {"model": OversizedRelationSchemaSchema},
        status.HTTP_409_CONFLICT: {"model": NotUniqueRelationNumberSchema},
    },
    summary="Create relation",
    description="Create relation with unique number.",
    tags=[Tag.relation],
)
@inject
async def _(
    create_relation: FromDishka[CreateRelation],
    relation_number: Annotated[int, Ge(0)],
    request_body: CreateRelationSchema,
) -> Response:
    await create_relation(
        Number(relation_number),
        request_body.schema_.decoded(),
    )

    return Response(status_code=status.HTTP_201_CREATED)
