from typing import Annotated, Literal

from annotated_types import Ge
from pydantic import BaseModel, Field

from tgdb.application.relation.ports.tuples import OversizedRelationSchemaError
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID
from tgdb.entities.relation.tuple_effect import InvalidRelationTupleError


class NoRelationSchema(BaseModel):
    """
    Relation was not created.
    """

    type: Literal["noRelation"] = "noRelation"


class NotUniqueRelationNumberSchema(BaseModel):
    type: Literal["notUniqueRelationNumber"] = "notUniqueRelationNumber"


class OversizedRelationSchemaSchema(BaseModel):
    type: Literal["oversizedSchema"] = "oversizedSchema"
    schema_size: int = Field(alias="schemaSize")
    schema_max_size: int = Field(alias="schemaMaxSize")

    @classmethod
    def of(
        cls,
        error: OversizedRelationSchemaError,
    ) -> "OversizedRelationSchemaSchema":
        return OversizedRelationSchemaSchema(
            schemaSize=error.schema_size,
            schemaMaxSize=error.schema_max_size,
        )


class InvalidRelationTupleSchema(BaseModel):
    type: Literal["invalidRelationTuple"] = "invalidRelationTuple"
    tid: TID
    scalars: tuple[Scalar, ...]
    relation_number: Annotated[int, Ge(0)]

    @classmethod
    def of(
        cls,
        error: InvalidRelationTupleError,
    ) -> "InvalidRelationTupleSchema":
        return InvalidRelationTupleSchema(
            tid=error.tid,
            scalars=error.scalars,
            relation_number=int(error.relation_number),
        )
