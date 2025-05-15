from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Never, overload
from uuid import UUID

from effect import Effect, IdentifiedValue


type RowAttribute = bool | int | str | datetime | UUID | StrEnum


@dataclass(frozen=True)
class RowSchema(Sequence[type[RowAttribute]]):
    name: str
    id_type: type[RowAttribute]
    body_types: tuple[type[RowAttribute], ...] = tuple()

    def __iter__(self) -> Iterator[type[RowAttribute]]:
        yield self.id_type
        yield from self.body_types

    @overload
    def __getitem__(self, index: int, /) -> type[RowAttribute]: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[type[RowAttribute]]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[type[RowAttribute]] | type[RowAttribute]:
        return tuple(self)[value]

    def __len__(self) -> int:
        return len(self.body_types) + 1


class RowSchemaError(Exception):
    def __init__(self, schema: RowSchema) -> None:
        self.schema = schema
        super().__init__()


@dataclass(frozen=True)
class Row(IdentifiedValue[RowAttribute], Sequence[RowAttribute]):
    body: tuple[RowAttribute, ...]
    schema: RowSchema

    def __post_init__(self) -> None:
        for attribute_and_type in zip(self, self.schema, strict=False):
            if len(attribute_and_type) != 2:
                raise RowSchemaError(self.schema)

            attribute, type = attribute_and_type

            if not isinstance(attribute, type):
                raise RowSchemaError(self.schema)

    def __iter__(self) -> Iterator[RowAttribute]:
        yield self.id
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


type RowEffect = Effect[Row, Row, Never, Row, Row]
