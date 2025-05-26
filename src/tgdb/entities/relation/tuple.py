from collections.abc import Iterator
from dataclasses import dataclass

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar


@dataclass(frozen=True)
class TupleID:
    relation_id: Number
    scalar: Scalar


@dataclass(frozen=True)
class Tuple:
    id: TupleID
    scalars: tuple[Scalar, ...]

    def __iter__(self) -> Iterator[Scalar]:
        return iter(self.scalars)


def tuple_(*scalars: Scalar, relation_id: Number = Number(0)) -> "Tuple":  # noqa: B008
    return Tuple(TupleID(relation_id, scalars[0]), scalars[1:])
