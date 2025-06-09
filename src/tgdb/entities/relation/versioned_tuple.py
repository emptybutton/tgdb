from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.tuple import Tuple
from tgdb.entities.tools.assert_ import assert_


class HeterogeneousVersionedTupleError(Exception): ...


@dataclass(frozen=True)
class VersionedTuple:
    map: Mapping[Number, Tuple]

    def last_version(self) -> Tuple:
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
        :raises tgdb.entities.relation.versioned_tuple.HeterogeneousVersionedTupleError:
        """  # noqa: E501

        tid_set = frozenset(version.tid for version in self.map.values())
        assert_(len(tid_set) == 1, else_=HeterogeneousVersionedTupleError)


def versioned_tuple(tuple_: Tuple) -> VersionedTuple:
    return VersionedTuple({Number(0): tuple_})
