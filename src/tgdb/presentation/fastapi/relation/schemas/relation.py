from collections.abc import Iterable
from uuid import UUID

from pydantic import BaseModel, Field, PositiveInt

from tgdb.entities.relation.relation import (
    DerivativeRelationVersion,
    InitialRelationVersion,
    Relation,
)
from tgdb.presentation.fastapi.relation.schemas.schema import SchemaSchema


class InitialRelationVersionSchema(BaseModel):
    number: PositiveInt
    schema_: SchemaSchema = Field(alias="schema")

    @classmethod
    def of(cls, version: InitialRelationVersion) -> (
        "InitialRelationVersionSchema"
    ):
        return InitialRelationVersionSchema(
            number=int(version.number),
            schema=SchemaSchema.of(version.schema),
        )


class DerivativeRelationVersionSchema(BaseModel):
    number: PositiveInt
    schema_: SchemaSchema = Field(alias="schema")
    migration_id: UUID = Field(alias="migrationID")

    @classmethod
    def of(cls, version: DerivativeRelationVersion) -> (
        "DerivativeRelationVersionSchema"
    ):
        return DerivativeRelationVersionSchema(
            number=int(version.number),
            schema=SchemaSchema.of(version.schema),
            migrationID=version.migration_id,
        )


class RelationSchema(BaseModel):
    number: PositiveInt
    initial_version: InitialRelationVersionSchema = Field(
        alias="initialVersion"
    )
    intermediate_versions: tuple[DerivativeRelationVersionSchema, ...] = Field(
        alias="intermediateVersions"
    )

    @classmethod
    def of(cls, relation: Relation) -> "RelationSchema":
        initial_version = (
            InitialRelationVersionSchema.of(relation.initial_version())
        )
        intermediate_versions = tuple(map(
            DerivativeRelationVersionSchema.of,
            relation.intermediate_versions(),
        ))

        return RelationSchema(
            number=int(relation.number()),
            initialVersion=initial_version,
            intermediateVersions=intermediate_versions,
        )


class RelationListSchema(BaseModel):
    relation_numbers: tuple[PositiveInt, ...] = Field(alias="relationNumbers")

    @classmethod
    def of(cls, relations: Iterable[Relation]) -> "RelationListSchema":
        relation_numbers = tuple(map(int, map(Relation.number, relations)))

        return RelationListSchema(relationNumbers=relation_numbers)
