from typing import Annotated

from annotated_types import Ge
from pydantic import BaseModel

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import RelationSchemaID
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID, Tuple


class EncodableTuple(BaseModel):
    tid: TID
    relation_number: Annotated[int, Ge(0)]
    relation_version_number: Annotated[int, Ge(0)]
    scalars: tuple[Scalar, ...]

    def entity(self) -> Tuple:
        schema_id = RelationSchemaID(
            Number(self.relation_number),
            Number(self.relation_version_number),
        )

        return Tuple(self.tid, schema_id, self.scalars)

    @classmethod
    def of(cls, entity: Tuple) -> "EncodableTuple":
        return EncodableTuple(
            tid=entity.tid,
            relation_number=int(entity.relation_schema_id.relation_number),
            relation_version_number=int(
                entity.relation_schema_id.relation_version_number,
            ),
            scalars=entity.scalars,
        )
