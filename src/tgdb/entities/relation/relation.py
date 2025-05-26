from dataclasses import dataclass

from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.schema import Schema


@dataclass(frozen=True)
class UniversalRelation:
    schema: Schema

    def __bool__(self) -> bool:
        return bool(self.schema)

    def __contains__(self, tuple_: tuple[Scalar, ...]) -> bool:
        if not self.schema:
            return False

        if len(tuple_) != len(self.schema):
            return False

        return all(
            scalar in domain
            for scalar, domain in zip(tuple_, self.schema, strict=True)
        )
