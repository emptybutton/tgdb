from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse, Response

from tgdb.application.relation.view_all_relations import ViewAllRelations
from tgdb.presentation.fastapi.schemas.relation.relation import (
    RelationListSchema,
    RelationSchema,
)
from tgdb.presentation.fastapi.tags import Tag


view_relation_router = APIRouter()


@view_relation_router.get(
    "/relations",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_200_OK: {"model": RelationListSchema},
    },
    summary="View all relations",
    description="View all relation numbers.",
    tags=[Tag.relation],
)
@inject
async def _(
    view_all_relations: FromDishka[
        ViewAllRelations[RelationListSchema, RelationSchema | None]
    ],
) -> Response:
    view = await view_all_relations()

    response_body = view.model_dump(mode="json", by_alias=True)
    return JSONResponse(response_body, status_code=status.HTTP_200_OK)
