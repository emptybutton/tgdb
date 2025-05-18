from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, overload
from uuid import UUID

from effect import Dead, IdentifiedValue, Mutated, New


type RowAttribute = bool | int | str | datetime | UUID | StrEnum


@dataclass(frozen=True)
class Row(IdentifiedValue[RowAttribute], Sequence[RowAttribute]):
    body: tuple[RowAttribute, ...]

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


def row(*attrs: RowAttribute) -> Row:
    return Row(attrs[0], attrs[1:])


type RowEffect = New[Row] | Mutated[Row] | Dead[Row]
