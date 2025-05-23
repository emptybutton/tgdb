from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, overload
from uuid import UUID

from tgdb.entities.message import Message


type RowAttribute = None | bool | int | str | datetime | UUID  # noqa: RUF036


@dataclass(frozen=True)
class AttributeSchema:
    type_: type[bool | int | str | datetime | UUID]
    is_nonable: bool

    def describes(self, attribute: RowAttribute) -> bool:
        if attribute is None:
            return self.is_nonable

        return isinstance(attribute, self.type_)


@dataclass(frozen=True)
class SchemaID:
    schema_name: str


@dataclass(frozen=True)
class SchemaVersionID:
    schema_id: SchemaID
    schema_version_number: int


@dataclass(frozen=True, unsafe_hash=False, eq=False)
class SchemaVersion(Sequence[AttributeSchema]):
    schema_id: SchemaID
    number: int
    row_id_schema: AttributeSchema
    row_body_schema: tuple[AttributeSchema, ...]

    def id(self) -> SchemaVersionID:
        return SchemaVersionID(self.schema_id, self.number)

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

    def __iter__(self) -> Iterator[AttributeSchema]:
        yield self.row_id_schema
        yield from self.row_body_schema

    @overload
    def __getitem__(self, index: int, /) -> AttributeSchema: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[AttributeSchema]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[AttributeSchema] | AttributeSchema:
        return tuple(self)[value]

    def __len__(self) -> int:
        return len(self.row_body_schema) + 1


class NotIncrementedSchemaVersionsError(Exception): ...


class SchemaWithoutVersionsError(Exception): ...


class UselessSchemaVersionError(Exception): ...


@dataclass(frozen=True, unsafe_hash=False)
class Schema:
    name: str
    _versions: list[SchemaVersion]

    def __post_init__(self) -> None:
        if not self._versions:
            raise SchemaWithoutVersionsError

        for version_index in range(len(self._versions) - 1):
            version = self._versions[version_index]
            next_version = self._versions[version_index]

            self._validate_increment(version, next_version)
            self._validate_useless(version, next_version)

    def id(self) -> SchemaID:
        return SchemaID(self.name)

    def last_version(self) -> SchemaVersion:
        return self._versions[-1]

    def old_versions(self) -> Sequence[SchemaVersion]:
        return self._versions[:-1]

    def add_version(
        self,
        row_id_schema: AttributeSchema,
        row_body_schema: tuple[AttributeSchema, ...],
    ) -> None:
        new_version = SchemaVersion(
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
        self, prevous_version: SchemaVersion, new_version: SchemaVersion
    ) -> None:
        if prevous_version.number + 1 != new_version.number:
            raise NotIncrementedSchemaVersionsError

    def _validate_useless(
        self, prevous_version: SchemaVersion, new_version: SchemaVersion
    ) -> None:
        if prevous_version == new_version:
            raise UselessSchemaVersionError


@dataclass(frozen=True, repr=False)
class RowID:
    attribute: RowAttribute
    schema_id: SchemaID


@dataclass(frozen=True, repr=False)
class RowVersionID:
    attribute: RowAttribute
    schema_version_id: SchemaVersionID


@dataclass(frozen=True)
class RowVersion(Sequence[RowAttribute]):
    id: RowVersionID
    body: tuple[RowAttribute, ...]

    def row_id(self) -> RowID:
        return RowID(self.id.attribute, self.id.schema_version_id.schema_id)

    def __iter__(self) -> Iterator[RowAttribute]:
        yield self.id.attribute
        yield from self.body

    @overload
    def __getitem__(self, index: int, /) -> RowAttribute: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[RowAttribute]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[RowAttribute] | RowAttribute:
        return tuple(self)[value]

    def __len__(self) -> int:
        return len(self.body) + 1


@dataclass(frozen=True)
class Row:
    id: RowID
    versions: tuple[RowVersion, ...]


def row(*attrs: RowAttribute, schema_version_id: SchemaVersionID) -> Row:
    row_version_id = RowVersionID(attrs[0], schema_version_id)
    row_version_body = attrs[1:]

    row_version = RowVersion(row_version_id, row_version_body)
    row_id = row_version.row_id()

    return Row(row_id, (row_version, ))


@dataclass(frozen=True)
class NewRow:
    row_version: RowVersion

    @property
    def id(self) -> RowID:
        return self.row_version.row_id()


@dataclass(frozen=True)
class ViewedRow:
    id: RowID


# |---|
#  |========================|

# int int int nullable nullable


@dataclass(frozen=True)
class MutatedRow:
    row_version: RowVersion
    message: Message | None

    @property
    def id(self) -> RowID:
        return self.row.id


@dataclass(frozen=True)
class DeletedRow:
    id: RowID
    message: Message | None


type RowEffect = NewRow | MutatedRow | DeletedRow | ViewedRow
