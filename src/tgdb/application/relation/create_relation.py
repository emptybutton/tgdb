from dataclasses import dataclass

from tgdb.application.relation.ports.relations import Relations
from tgdb.application.relation.ports.tuples import Tuples
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation
from tgdb.entities.relation.schema import Schema


@dataclass(frozen=True)
class CreateRelation:
    relations: Relations
    tuples: Tuples

    async def __call__(
        self,
        relation_number: Number,
        relation_schema: Schema,
    ) -> None:
        """
        :raises tgdb.application.relation.ports.relations.OversizedRelationSchemaError:
        :raises tgdb.application.relation.ports.relations.NotUniqueRelationNumberError:
        """  # noqa: E501

        new_relation = Relation.new(relation_number, relation_schema)
        await self.tuples.assert_can_accept_tuples(new_relation)

        await self.relations.add(new_relation)
