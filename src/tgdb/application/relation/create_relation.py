from dataclasses import dataclass

from tgdb.application.common.ports.relations import Relations
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation
from tgdb.entities.relation.schema import Schema


@dataclass(frozen=True)
class CreateRelation:
    relations: Relations

    async def __call__(
        self, relation_number: Number, relation_schema: Schema
    ) -> None:
        """
        :raises tgdb.application.common.ports.relations.NotUniqueRelationNumberError:
        """  # noqa: E501

        new_relation = Relation.new(relation_number, relation_schema)
        await self.relations.add(new_relation)
