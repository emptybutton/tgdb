from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.domain import Domain
from tgdb.entities.relation.schema import Schema
from tgdb.entities.topic.partition import (
    DerivativePartitionVersion,
    InitialPartitionVersion,
    Partition,
    PartitionVersion,
)


class TopicWithoutPartitionsError(Exception): ...


class InvalidPartitionMapError(Exception): ...


class NoPartitionError(Exception): ...


@dataclass
class Topic(Mapping[Number, Partition]):
    """
    :raises tgdb.entities.topic.topic.TopicWithoutPartitionsError:
    :raises tgdb.entities.topic.topic.InvalidPartitionMapError:
    """

    number: Number
    _partition_map: dict[Number, Partition]
    _max_partition_number: Number
    _id_domain: Domain

    def __post_init__(self) -> None:
        if not self._partition_map:
            raise TopicWithoutPartitionsError

        for number, partition in self._partition_map.items():
            if number != partition.number:
                raise InvalidPartitionMapError

    @classmethod
    def new(cls, number: Number, schema: Schema) -> "Topic":
        return Topic(
            number,
            {Number(0): Partition.new(Number(0), schema)},
            Number(0),
            schema[0],
        )

    def __len__(self) -> int:
        return len(self._partition_map)

    def __iter__(self) -> Iterator[Number]:
        return iter(self._partition_map)

    def __getitem__(self, number: Number) -> Partition:
        return self._partition_map[number]

    def add_partition(self, schema: Schema) -> None:
        new_partition = Partition.new(
            next(self._last_partition().number), schema
        )
        self._partition_map[new_partition.number] = new_partition
        self._max_partition_number = new_partition.number

    def remove_partition(self, partition_number: Number) -> None:
        """
        :raises tgdb.entities.topic.topic.NoPartitionError:
        """

        try:
            del self._partition_map[partition_number]
        except KeyError as error:
            raise NoPartitionError from error

    def partition_inital_version(
        self, partition_number: Number
    ) -> InitialPartitionVersion | None:
        partition = self._partition_map.get(partition_number)

        if partition is None:
            return None

        return partition.inital_version()

    def partition_intermediate_versions(
        self, partition_number: Number
    ) -> Sequence[DerivativePartitionVersion] | None:
        partition = self._partition_map.get(partition_number)

        if partition is None:
            return None

        return partition.intermediate_versions()

    def partition_last_version(
        self, partition_number: Number
    ) -> PartitionVersion | None:
        partition = self._partition_map.get(partition_number)

        if partition is None:
            return None

        return partition.last_version()

    def partition_recent_versions(
        self, partition_number: Number, current_version_number: Number
    ) -> Sequence[DerivativePartitionVersion] | None:
        partition = self._partition_map.get(partition_number)

        if partition is None:
            return None

        return partition.recent_versions(current_version_number)

    def migrate_partition(
        self,
        partition_number: Number,
        new_schema: Schema,
        migration_id: UUID,
    ) -> None:
        """
        :raises tgdb.entities.topic.topic.NoPartitionError:
        """

        partition = self._partition_map.get(partition_number)

        if partition is None:
            raise NoPartitionError

        partition.migrate(new_schema, migration_id)

    def remove_old_partition_versions(
        self,
        partition_number: Number,
        count: int,
    ) -> None:
        """
        :raises tgdb.entities.topic.topic.NoPartitionError:
        """

        partition = self._partition_map.get(partition_number)

        if partition is None:
            raise NoPartitionError

        partition.remove_old_versions(count)

    def _last_partition(self) -> Partition:
        return self._partition_map[max(self._partition_map)]


@dataclass(frozen=True)
class TopicSchemaID:
    topic_number: Number
    partition_number: Number
    partition_version_number: Number
