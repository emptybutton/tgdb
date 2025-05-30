from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation


class RelationViews[ViewOfAllRelationsT, ViewOfOneRelationT](ABC):
    @abstractmethod
    async def view_of_all_relations(self) -> ViewOfAllRelationsT: ...

    @abstractmethod
    async def view_of_one_relation(
        self, relation_number: Number
    ) -> ViewOfOneRelationT: ...
