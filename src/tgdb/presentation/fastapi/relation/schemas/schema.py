from pydantic import BaseModel

from tgdb.entities.relation.schema import Schema
from tgdb.presentation.fastapi.relation.schemas.domain import (
    DomainSchema,
    domain_schema,
)


class SchemaSchema(BaseModel):
    domains: tuple[DomainSchema, ...]

    def decoded(self) -> Schema:
        return tuple(domain.decoded() for domain in self.domains)

    @classmethod
    def of(cls, schema: Schema) -> "SchemaSchema":
        domain_schemas = tuple(map(domain_schema, schema))

        return SchemaSchema(domains=domain_schemas)
