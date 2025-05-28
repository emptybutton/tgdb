from collections.abc import Iterator
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import RelationVersionID
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.schema import Schema


type TID = UUID


@dataclass(frozen=True)
class Tuple:
    tid: TID
    relation_version_id: RelationVersionID
    scalars: tuple[Scalar, ...]

    def __iter__(self) -> Iterator[Scalar]:
        return iter(self.scalars)

    def __len__(self) -> int:
        return len(self.scalars)

    def matches(self, schema: Schema) -> bool:
        if len(schema) != len(self):
            return False

        return all(
            scalar in domain
            for scalar, domain in zip(self, schema, strict=True)
        )


def tuple_(
    *scalars: Scalar,
    tid: TID,
    relation_version_id: RelationVersionID = RelationVersionID(  # noqa: B008
        Number(0), Number(0)  # noqa: B008
    ),
) -> Tuple:
    return Tuple(tid, relation_version_id, scalars)
