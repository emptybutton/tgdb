from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import pairwise
from uuid import UUID

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.schema import Schema
from tgdb.entities.topic.row import RowID


@dataclass(frozen=True)
class InitialPartitionVersion:
    number: Number
    schema: Schema


@dataclass(frozen=True)
class DerivativePartitionVersion:
    number: Number
    schema: Schema
    migration_id: UUID


type PartitionVersion = InitialPartitionVersion | DerivativePartitionVersion


class NotIncrementedPartitionVersionError(Exception): ...


class PartitionWithoutLastVersionError(Exception): ...


@dataclass
class Partition:
    """
    :raises tgdb.entities.topic.partition.PartitionWithoutLastVersionError:
    :raises tgdb.entities.topic.partition.NotIncrementedPartitionVersionError:
    """

    number: Number
    _inital_version: InitialPartitionVersion
    _intermediate_versions: list[DerivativePartitionVersion]

    def __post_init__(self) -> None:
        if not self._intermediate_versions or not self._inital_version:
            raise PartitionWithoutLastVersionError

        for version, next_version in pairwise(self._versions()):
            if next(version.number) != next_version.number:
                raise NotIncrementedPartitionVersionError

    def __len__(self) -> int:
        return len(self._intermediate_versions) + 1

    def __iter__(self) -> Iterator[PartitionVersion]:
        return iter(self._versions())

    def inital_version(self) -> InitialPartitionVersion | None:
        return self._inital_version

    def intermediate_versions(self) -> Sequence[DerivativePartitionVersion]:
        return self._intermediate_versions

    def last_version(
        self
    ) -> InitialPartitionVersion | DerivativePartitionVersion:
        return (
            self._intermediate_versions[-1]
            if self._intermediate_versions
            else self._inital_version
        )

    def recent_versions(
        self, current_version_number: Number
    ) -> Sequence[DerivativePartitionVersion]:
        if current_version_number < self._inital_version.number:
            return tuple()

        if current_version_number == self._inital_version.number:
            return self._intermediate_versions

        if current_version_number >= self._intermediate_versions[-1].number:
            return tuple()

        if not self._intermediate_versions:
            return tuple()

        current_version_index = (
            int(self._intermediate_versions[0].number)
            - int(current_version_number)
        )

        return self._intermediate_versions[current_version_index + 1:]

    def migrate(
        self,
        new_version_schema: Schema,
        new_version_migration_id: UUID,
    ) -> None:
        last_version = DerivativePartitionVersion(
            next(self.last_version().number),
            new_version_schema,
            new_version_migration_id,
        )
        self._intermediate_versions.append(last_version)

    def remove_old_versions(self, count: int) -> None:
        count_to_remove_intermediate_versions = count - 1
        count_to_remove_intermediate_versions = (
            min(count_to_remove_intermediate_versions, len(self) - 1)
        )
        del self._intermediate_versions[:count_to_remove_intermediate_versions]

        version_to_make_inital = self._intermediate_versions[0]

        self._inital_version = InitialPartitionVersion(
            version_to_make_inital.number,
            version_to_make_inital.schema,
        )
        del self._intermediate_versions[0]

    @classmethod
    def new(cls, number: Number, schema: Schema) -> "Partition":
        return Partition(
            number,
            InitialPartitionVersion(Number(0), schema),
            list(),
        )

    def _versions(self) -> Sequence[PartitionVersion]:
        if self._inital_version:
            return (self._inital_version, *self._intermediate_versions)

        return self._intermediate_versions


@dataclass(frozen=True)
class PartitionVersionID:
    partition_number: Number
    partition_version_number: Number


@dataclass(frozen=True)
class PartitionTupleID:
    row_id: RowID
    partition_version_id: PartitionVersionID


@dataclass(frozen=True)
class PartitionTuple:
    id: PartitionTupleID
    scalars: tuple[Scalar, ...]

    def __iter__(self) -> Iterator[Scalar]:
        return iter(self.scalars)
