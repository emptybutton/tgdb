from pydantic import BaseModel

from tgdb.entities.relation.schema import Schema
from tgdb.infrastructure.pydantic.relation.domain import (
    EncodableDomain,
    encodable_domain,
)


class EncodableSchema(BaseModel):
    domains: tuple[EncodableDomain, ...]

    def entity(self) -> Schema:
        return tuple(domain.entity() for domain in self.domains)

    @classmethod
    def of(cls, schema: Schema) -> "EncodableSchema":
        encodable_domains = tuple(map(encodable_domain, schema))

        return EncodableSchema(domains=encodable_domains)
