from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, overload
from uuid import UUID


type Attribute = None | bool | int | str | datetime | UUID  # noqa: RUF036


class Domain(ABC):
    @abstractmethod
    def type(self) -> type[bool | int | str | datetime | UUID]: ...

    @abstractmethod
    def is_nonable(self) -> bool: ...

    @abstractmethod
    def __contains__(self, attribute: Attribute) -> bool: ...


@dataclass(frozen=True)
class IntDomain(Domain):
    min: int
    max: int
    _is_nonable: bool

    def type(self) -> type[int]:
        return int

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, attribute: Attribute) -> bool:
        return isinstance(attribute, int) and self.min <= attribute <= self.max


@dataclass(frozen=True)
class StrDomain(Domain):
    max_len: int
    _is_nonable: bool

    def type(self) -> type[str]:
        return str

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, attribute: Attribute) -> bool:
        return isinstance(attribute, str) and len(attribute) <= self.max_len


@dataclass(frozen=True)
class SetDomain(Domain):
    values: tuple[bool | int | str | datetime | UUID, ...]
    _is_nonable: bool

    def type(self) -> type[bool | int | str | datetime | UUID]:
        return str

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, attribute: Attribute) -> bool:
        return attribute in self.values


@dataclass(frozen=True)
class TypeDomain(Domain):
    _type: type[bool | datetime | UUID]
    _is_nonable: bool

    def type(self) -> type[bool | datetime | UUID]:
        return self._type

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, attribute: Attribute) -> bool:
        if attribute is None:
            return self._is_nonable

        return isinstance(attribute, self._type)


class NegativeNumberError(Exception): ...


@dataclass(frozen=True)
class Number:
    int: int

    def __post_init__(self) -> None:
        if self.int < 0:
            raise NegativeNumberError

    def __int__(self) -> "int":
        return self.int


@dataclass(frozen=True, unsafe_hash=False, eq=False)
class SchemaPartitionVersion(Sequence[Domain]):  # noqa: PLW1641
    number: int
    row_id_schema: Domain
    row_body_schema: tuple[Domain, ...]

    def describes(self, row: "RowVersion") -> bool:
        if len(self) != len(row):
            return False

        return all(
            attribute_schema.describes(attribute)
            for attribute_schema, attribute in zip(self, row, strict=True)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SchemaVersion):
            return False

        if len(self) != len(other):
            return False

        return all(
            schema1 == schema2
            for schema1, schema2 in zip(self, other, strict=True)
        )

    def __iter__(self) -> Iterator[Domain]:
        yield self.row_id_schema
        yield from self.row_body_schema

    @overload
    def __getitem__(self, index: int, /) -> Domain: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[Domain]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[Domain] | Domain:
        return tuple(self)[value]

    def __len__(self) -> int:
        return len(self.row_body_schema) + 1


# |1%1#0=123|


@dataclass(frozen=True, unsafe_hash=False)
class Schema:
    number: int
    _partitions: list[SchemaPartition]

    def __post_init__(self) -> None:
        if not self._versions:
            raise SchemaWithoutVersionsError

        for version_index in range(len(self._versions) - 1):
            version = self._versions[version_index]
            next_version = self._versions[version_index]

            self._validate_increment(version, next_version)


class NotIncrementedSchemaVersionsError(Exception): ...


class SchemaWithoutVersionsError(Exception): ...


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

    def id(self) -> SchemaID:
        return SchemaID(self.number)

    def last_version(self) -> SchemaPartitionVersion:
        return self._versions[-1]

    def old_versions(self) -> Sequence[SchemaPartitionVersion]:
        return self._versions[:-1]

    def add_version(
        self,
        row_id_schema: Domain,
        row_body_schema: tuple[Domain, ...],
    ) -> None:
        new_version = SchemaPartitionVersion(
            self.id(),
            self.last_version().number + 1,
            row_id_schema,
            row_body_schema,
        )
        self._versions.append(new_version)

    def remove_old_versions(self, *, count: int) -> None:
        if count >= len(self._versions):
            raise SchemaWithoutVersionsError

        for _ in range(count):
            self._versions.pop(0)

    def _validate_increment(
        self, prevous_version: SchemaPartitionVersion, new_version: SchemaPartitionVersion
    ) -> None:
        if prevous_version.number + 1 != new_version.number:
            raise NotIncrementedSchemaVersionsError


@dataclass(frozen=True, repr=False)
class RowID:
    attribute: Attribute
    schema_id: SchemaID


@dataclass(frozen=True, repr=False)
class RowVersionID:
    attribute: Attribute
    schema_version_id: SchemaVersionID


@dataclass(frozen=True)
class RowVersion(Sequence[Attribute]):
    id: RowVersionID
    body: tuple[Attribute, ...]

    def row_id(self) -> RowID:
        return RowID(self.id.attribute, self.id.schema_version_id.schema_id)

    def __iter__(self) -> Iterator[Attribute]:
        yield self.id.attribute
        yield from self.body

    @overload
    def __getitem__(self, index: int, /) -> Attribute: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[Attribute]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[Attribute] | Attribute:
        return tuple(self)[value]

    def __len__(self) -> int:
        return len(self.body) + 1


@dataclass(frozen=True)
class Row:
    id: RowID
    versions: tuple[RowVersion, ...]


def row(*attrs: Attribute, schema_version_id: SchemaVersionID) -> Row:
    row_version_id = RowVersionID(attrs[0], schema_version_id)
    row_version_body = attrs[1:]

    row_version = RowVersion(row_version_id, row_version_body)
    row_id = row_version.row_id()

    return Row(row_id, (row_version, ))
