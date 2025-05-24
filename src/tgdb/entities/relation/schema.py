from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, overload

from tgdb.entities.number import Number
from tgdb.entities.relation.domain import Domain
from tgdb.entities.relation.scalar import Scalar


@dataclass(frozen=True, unsafe_hash=False, eq=False)
class SchemaPartitionVersion:  # noqa: PLW1641
    number: Number
    domains: tuple[Domain, ...]

    def __contains__(self, tuple_: tuple[Scalar, ...]) -> bool:
        if len(self) != len(tuple_):
            return False

        return all(
            scalar in domain
            for domain, scalar in zip(self, tuple_, strict=True)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SchemaPartitionVersion):
            return False

        if len(self) != len(other):
            return False

        return all(
            schema1 == schema2
            for schema1, schema2 in zip(self, other, strict=True)
        )

    def __iter__(self) -> Iterator[Domain]:
        return iter(self.domains)

    @overload
    def __getitem__(self, index: int, /) -> Domain: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[Domain]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[Domain] | Domain:
        return self.domains[value]

    def __len__(self) -> int:
        return len(self.domains)


@dataclass(frozen=True, unsafe_hash=False)
class SchemaPartition:
    number: int
    _versions: list[SchemaPartitionVersion]

    def __post_init__(self) -> None:
        if not self._versions:
            raise SchemaWithoutVersionsError

        for version_index in range(len(self._versions) - 1):
            version = self._versions[version_index]
            next_version = self._versions[version_index]

            self._validate_increment(version, next_version)

    def last_version(self) -> SchemaPartitionVersion:
        return self._versions[-1]

    def old_versions(self) -> Sequence[SchemaPartitionVersion]:
        return self._versions[:-1]

    def add_version(self, domains: tuple[Domain, ...]) -> None:
        new_version = SchemaPartitionVersion(
            self.last_version().number + 1, domains,
        )
        self._versions.append(new_version)

    def remove_old_versions(self, *, count: int) -> None:
        if count >= len(self._versions):
            raise SchemaWithoutVersionsError

        for _ in range(count):
            self._versions.pop(0)

    def _validate_increment(
        self,
        prevous_version: SchemaPartitionVersion,
        new_version: SchemaPartitionVersion,
    ) -> None:
        if prevous_version.number + 1 != new_version.number:
            raise NotIncrementedSchemaVersionsError


class NotIncrementedSchemaVersionsError(Exception): ...


# | id int | id str |


@dataclass(frozen=True, unsafe_hash=False)
class Schema:
    number: Number
    _partitions: list[SchemaPartition]

    def __post_init__(self) -> None:
        if not self._partitions:
            raise SchemaWithoutVersionsError

        for version_index in range(len(self._partitions) - 1):
            version = self._partitions[version_index]
            next_version = self._partitions[version_index]

            self._validate_increment(version, next_version)


class NotIncrementedSchemaVersionsError(Exception): ...


class SchemaWithoutVersionsError(Exception): ...

