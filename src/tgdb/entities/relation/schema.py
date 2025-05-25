from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, overload

from tgdb.entities.relation.domain import Domain
from tgdb.entities.relation.scalar import Scalar


class EmptyRelationSchemaError(Exception): ...


@dataclass(frozen=True)
class Schema:
    """
    :raises tgdb.entities.relation.schema.EmptyRelationSchemaError:
    """

    domains: tuple[Domain, ...]

    def __post_init__(self) -> None:
        if not self.domains:
            raise EmptyRelationSchemaError

    def __contains__(self, tuple_: tuple[Scalar, ...]) -> bool:
        if len(self) != len(tuple_):
            return False

        return all(
            scalar in domain
            for domain, scalar in zip(self, tuple_, strict=True)
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
