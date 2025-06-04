from typing import Literal

from pydantic import BaseModel, Field

from tgdb.application.common.ports.tuples import OversizedRelationSchemaError


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
        cls, error: OversizedRelationSchemaError
    ) -> "OversizedRelationSchemaSchema":
        return OversizedRelationSchemaSchema(
            schemaSize=error.schema_size,
            schemaMaxSize=error.schema_max_size,
        )
