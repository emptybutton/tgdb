from collections.abc import Iterable
from dataclasses import dataclass

from in_memory_db import InMemoryDb

from tgdb.application.relation.ports.relation_views import RelationViews
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation
from tgdb.presentation.fastapi.relation.schemas.relation import (
    RelationListSchema,
    RelationSchema,
)


@dataclass(frozen=True, unsafe_hash=False)
class RelationsFromInMemoryDbAsRelationViews(
    RelationViews[Iterable[Relation], Relation | None],
):
    _relations: InMemoryDb[Relation]

    async def view_of_all_relations(self) -> Iterable[Relation]:
        return iter(self._relations)

    async def view_of_one_relation(
        self,
        relation_number: Number,
    ) -> Relation | None:
        return self._relations.select_one(
            lambda it: (it.number() == relation_number),
        )


@dataclass(frozen=True, unsafe_hash=False)
class RelationSchemasFromInMemoryDbAsRelationViews(
    RelationViews[RelationListSchema, RelationSchema | None],
):
    _relations: InMemoryDb[Relation]

    async def view_of_all_relations(self) -> RelationListSchema:
        return RelationListSchema.of(iter(self._relations))

    async def view_of_one_relation(
        self,
        relation_number: Number,
    ) -> RelationSchema | None:
        relation = self._relations.select_one(
            lambda it: (it.number() == relation_number),
        )

        if relation is None:
            return None

        return RelationSchema.of(relation)
