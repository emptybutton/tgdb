from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import batched, pairwise
from uuid import UUID

from tgdb.entities.number import Number
from tgdb.entities.relation.domain import Domain
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.schema import Schema


@dataclass(frozen=True)
class PartitionVersion:
    number: Number
    schema: Schema
    migration_id: UUID


class NotIncrementedPartitionVersionError(Exception): ...


class PartitionWithoutVersionsError(Exception): ...


# UUID @ str | bool int

#  00000000000000000000000000000001:0:0:qweqweqweqwe oaksdoaksd qweq
#  00000000000000000000000000000001:0:1:


@dataclass(frozen=True)
class Partition:
    """
    :raises tgdb.entities.relation.schema.PartitionWithoutVersionsError:
    :raises tgdb.entities.relation.schema.NotIncrementedPartitionVersionError:
    """

    number: Number
    _version_map: OrderedDict[Number, PartitionVersion]

    def __post_init__(self) -> None:
        if not self._version_map:
            raise PartitionWithoutVersionsError

        for number, next_number in pairwise(self._version_map):
            if next(number) != next_number:
                raise NotIncrementedPartitionVersionError

    def last_version(self) -> PartitionVersion:
        return self._version_map.keys()

    def old_versions(self) -> Sequence[PartitionVersion]:
        return self._versions[:-1]

    def migrate(
        self,
        new_version_schema: Schema,
        new_version_migration_id: UUID,
    ) -> None:
        new_version = PartitionVersion(
            next(self.last_version().number),
            new_version_schema,
            new_version_migration_id,
        )
        self._versions.append(new_version)

    def remove_old_versions(self, *, count: int) -> None:
        if count >= len(self._versions):
            raise PartitionWithoutVersionsError

        for _ in range(count):
            self._versions.pop(0)

    @classmethod
    def new(cls, number: Number, schema: Schema) -> "Partition":
        return Partition(number, [PartitionVersion(Number(0), schema)])


class EntityWithoutPartitionsError(Exception): ...


class DifferentlyIdentifyingPartitionError(Exception): ...


class InvalidPartitionMapError(Exception): ...


class NoPartitionError(Exception): ...


@dataclass(frozen=True)
class Entity:
    """
    :raises tgdb.entities.entity.EntityWithoutPartitionsError:
    :raises tgdb.entities.entity.InvalidPartitionMapError:
    """

    number: Number
    _partition_map: dict[Number, Partition]

    def __post_init__(self) -> None:
        if not self._partition_map:
            raise EntityWithoutPartitionsError

        for number, partition in self._partition_map.items():
            if number != partition.number:
                raise InvalidPartitionMapError

    @classmethod
    def new(cls, number: Number, schema: Schema) -> "Entity":
        return Entity(number, {Number(0): Partition.new(Number(0), schema)})

    def add_partition(self, schema: Schema) -> None:
        new_partition = Partition.new(
            next(self._last_partition().number), schema
        )
        self._partition_map[new_partition.number] = new_partition

    def partition_numbers(self) -> tuple[Number, ...]:
        return tuple(self._partition_map.keys())

    def partition_last_version(
        self, partition_number: Number
    ) -> PartitionVersion | None:
        partition = self._partition_map.get(partition_number)

        if partition is None:
            return None

        return partition.last_version()

    def partition_old_versions(
        self, partition_number: Number
    ) -> Sequence[PartitionVersion] | None:
        partition = self._partition_map.get(partition_number)

        if partition is None:
            return None

        return partition.old_versions()

    def migrate_partition(
        self,
        partition_number: Number,
        new_schema: Schema,
        migration_id: UUID,
    ) -> None:
        """
        :raises tgdb.entities.entity.NoPartitionError:
        :raises tgdb.entities.entity.DifferentlyIdentifyingPartitionError:
        """

        partition = self._partition_map.get(partition_number)

        if partition is None:
            raise NoPartitionError

        partition

        return partition.old_versions()

    def remove_old_versions(self, *, count: int) -> None:
        if count >= len(self._versions):
            raise PartitionWithoutVersionsError

        for _ in range(count):
            self._versions.pop(0)

    def _last_partition(self) -> Partition:
        return self._partition_map[max(self._partition_map)]
