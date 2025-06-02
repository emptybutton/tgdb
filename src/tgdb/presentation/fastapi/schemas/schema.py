from pydantic import BaseModel

from tgdb.entities.relation.schema import Schema
from tgdb.presentation.fastapi.schemas.domain import DomainSchema
from tgdb.presentation.fastapi.schemas.encoding import EncodingSchema


class SchemaSchema(EncodingSchema[Schema]):
    domains: tuple[DomainSchema, ...]

    def decoded(self) -> Schema:
        return tuple(domain.decoded() for domain in self.domains)
