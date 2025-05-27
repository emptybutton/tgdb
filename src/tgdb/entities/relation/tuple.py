from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.tools.assert_ import assert_


@dataclass(frozen=True)
class TupleID:
    relation_id: Number
    scalar: Scalar


@dataclass(frozen=True)
class Tuple:
    id: TupleID
    scalars: tuple[Scalar, ...]

    def __iter__(self) -> Iterator[Scalar]:
        yield self.id.scalar
        yield from iter(self.scalars)

    def __len__(self) -> int:
        return len(self.scalars) + 1


class HeterogeneousVersionedTupleError(Exception): ...


@dataclass(frozen=True)
class VersionedTuple:
    map: Mapping[Number, Tuple]

    def latest_version(self) -> Tuple:
        return self.map[max(self.map)]

    def old_versions(self) -> Sequence[Tuple]:
        latest_version_number = max(self.map)

        return tuple(
            self.map[latest_version_number]
            for number in self.map
            if number != latest_version_number
        )

    def __post_init__(self) -> None:
        """
        :raises tgdb.entities.relation.tuple.HeterogeneousVersionedTupleError:
        """

        id_set = frozenset(version.id for version in self.map.values())
        assert_(len(id_set) == 1, else_=HeterogeneousVersionedTupleError)


def tuple_(*scalars: Scalar, relation_id: Number = Number(0)) -> "Tuple":  # noqa: B008
    return Tuple(TupleID(relation_id, scalars[0]), scalars[1:])
