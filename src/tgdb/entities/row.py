from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, overload
from uuid import UUID

from tgdb.entities.message import Message


type RowAttribute = None | bool | int | str | datetime | UUID  # noqa: RUF036
type Schema = str


@dataclass(frozen=True, repr=False)
class Row(Sequence[RowAttribute]):
    id: RowAttribute
    body: tuple[RowAttribute, ...]
    schema: Schema

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

    def __repr__(self) -> str:
        return f"Row{tuple(self)}"


def row(*attrs: RowAttribute, schema: Schema = "__undefined__") -> Row:
    return Row(attrs[0], attrs[1:], schema)


@dataclass(frozen=True)
class NewRow:
    row: Row

    @property
    def id(self) -> RowAttribute:
        return self.row.id


@dataclass(frozen=True)
class ViewedRow:
    id: RowAttribute


@dataclass(frozen=True)
class MutatedRow:
    row: Row
    message: Message | None

    @property
    def id(self) -> RowAttribute:
        return self.row.id


@dataclass(frozen=True)
class DeletedRow:
    id: RowAttribute
    message: Message | None


type RowEffect = NewRow | MutatedRow | DeletedRow | ViewedRow
