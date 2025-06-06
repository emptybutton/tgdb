from dataclasses import dataclass

from tgdb.application.relation.ports.relation_views import RelationViews


@dataclass(frozen=True)
class ViewAllRelations[ViewOfAllRelationsT, ViewOfOneRelationT]:
    relation_views: RelationViews[ViewOfAllRelationsT, ViewOfOneRelationT]

    async def __call__(self) -> ViewOfAllRelationsT:
        return await self.relation_views.view_of_all_relations()
