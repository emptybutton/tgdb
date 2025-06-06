from dataclasses import dataclass

from tgdb.application.relation.ports.relation_views import RelationViews
from tgdb.entities.numeration.number import Number


@dataclass(frozen=True)
class ViewRelation[ViewOfAllRelationsT, ViewOfOneRelationT]:
    relation_views: RelationViews[ViewOfAllRelationsT, ViewOfOneRelationT]

    async def __call__(self, relation_number: Number) -> ViewOfOneRelationT:
        return await self.relation_views.view_of_one_relation(relation_number)
