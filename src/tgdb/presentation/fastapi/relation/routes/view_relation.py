from typing import Annotated

from annotated_types import Ge
from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse, Response

from tgdb.application.relation.view_relation import ViewRelation
from tgdb.entities.numeration.number import Number
from tgdb.presentation.fastapi.common.tags import Tag
from tgdb.presentation.fastapi.relation.schemas.relation import (
    RelationListSchema,
    RelationSchema,
)


view_relation_router = APIRouter()


@view_relation_router.get(
    "/relations/{relation_number}",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_200_OK: {"model": RelationSchema},
        status.HTTP_204_NO_CONTENT: {},
    },
    summary="View relation",
    description="View relation detail data.",
    tags=[Tag.relation],
)
@inject
async def _(
    view_relation: FromDishka[
        ViewRelation[RelationListSchema, RelationSchema | None]
    ],
    relation_number: Annotated[int, Ge(0)],
) -> Response:
    view = await view_relation(Number(relation_number))

    if view is None:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    response_body = view.model_dump(mode="json", by_alias=True)
    return JSONResponse(response_body, status_code=status.HTTP_200_OK)
