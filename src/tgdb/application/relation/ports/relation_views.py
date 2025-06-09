from abc import ABC, abstractmethod

from tgdb.entities.numeration.number import Number


class RelationViews[ViewOfAllRelationsT, ViewOfOneRelationT](ABC):
    @abstractmethod
    async def view_of_all_relations(self) -> ViewOfAllRelationsT: ...

    @abstractmethod
    async def view_of_one_relation(
        self,
        relation_number: Number,
    ) -> ViewOfOneRelationT: ...
