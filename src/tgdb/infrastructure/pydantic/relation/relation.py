from typing import Annotated
from uuid import UUID

from annotated_types import Ge
from pydantic import BaseModel

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import (
    DerivativeRelationVersion,
    InitialRelationVersion,
    Relation,
)
from tgdb.infrastructure.pydantic.relation.schema import EncodableSchema


class EncodableInitialRelationVersion(BaseModel):
    number: Annotated[int, Ge(0)]
    schema_: EncodableSchema

    def entity(self) -> InitialRelationVersion:
        return InitialRelationVersion(
            number=Number(self.number),
            schema=self.schema_.entity(),
        )

    @classmethod
    def of(
        cls,
        version: InitialRelationVersion,
    ) -> "EncodableInitialRelationVersion":
        return EncodableInitialRelationVersion(
            number=int(version.number),
            schema_=EncodableSchema.of(version.schema),
        )


class EncodableDerivativeRelationVersion(BaseModel):
    number: Annotated[int, Ge(0)]
    schema_: EncodableSchema
    migration_id: UUID

    def entity(self) -> DerivativeRelationVersion:
        return DerivativeRelationVersion(
            number=Number(self.number),
            schema=self.schema_.entity(),
            migration_id=self.migration_id,
        )

    @classmethod
    def of(
        cls,
        version: DerivativeRelationVersion,
    ) -> "EncodableDerivativeRelationVersion":
        return EncodableDerivativeRelationVersion(
            number=int(version.number),
            schema_=EncodableSchema.of(version.schema),
            migration_id=version.migration_id,
        )


class EncodableRelation(BaseModel):
    number: Annotated[int, Ge(0)]
    initial_version: EncodableInitialRelationVersion
    intermediate_versions: tuple[EncodableDerivativeRelationVersion, ...]

    def entity(self) -> Relation:
        return Relation(
            _number=Number(self.number),
            _initial_version=self.initial_version.entity(),
            _intermediate_versions=[
                it.entity() for it in self.intermediate_versions
            ],
        )

    @classmethod
    def of(cls, relation: Relation) -> "EncodableRelation":
        initial_version = EncodableInitialRelationVersion.of(
            relation.initial_version(),
        )
        intermediate_versions = tuple(
            map(
                EncodableDerivativeRelationVersion.of,
                relation.intermediate_versions(),
            ),
        )

        return EncodableRelation(
            number=int(relation.number()),
            initial_version=initial_version,
            intermediate_versions=intermediate_versions,
        )
