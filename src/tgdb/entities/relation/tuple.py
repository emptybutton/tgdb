from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, overload
from uuid import UUID

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import RelationSchemaID
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.schema import Schema


type TID = UUID


@dataclass(frozen=True)
class Tuple(Sequence[Scalar]):
    tid: TID
    relation_schema_id: RelationSchemaID
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

    @overload
    def __getitem__(self, index: int, /) -> Scalar: ...

    @overload
    def __getitem__(
        self,
        sclice: "slice[Any, Any, Any]",
        /,
    ) -> Sequence[Scalar]: ...

    def __getitem__(
        self,
        key: "int | slice[Any, Any, Any]",
        /,
    ) -> Scalar | Sequence[Scalar]:
        return self.scalars[key]


def tuple_(
    *scalars: Scalar,
    tid: TID,
    relation_schema_id: RelationSchemaID = RelationSchemaID(  # noqa: B008
        Number(0),  # noqa: B008
        Number(0),  # noqa: B008
    ),
) -> Tuple:
    return Tuple(tid, relation_schema_id, scalars)
