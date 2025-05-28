from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import pairwise
from uuid import UUID

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.schema import Schema


@dataclass(frozen=True)
class InitialRelationVersion:
    number: Number
    schema: Schema


@dataclass(frozen=True)
class DerivativeRelationVersion:
    number: Number
    schema: Schema
    migration_id: UUID


@dataclass(frozen=True)
class RelationVersionID:
    relation_number: Number
    relation_version_number: Number


type RelationVersion = InitialRelationVersion | DerivativeRelationVersion


class NotIncrementedRelationVersionError(Exception): ...


class RelationWithoutLastVersionError(Exception): ...


@dataclass
class Relation:
    """
    :raises tgdb.entities.relation.relation.RelationWithoutLastVersionError:
    :raises tgdb.entities.relation.relation.NotIncrementedRelationVersionError:
    """

    _number: Number
    _inital_version: InitialRelationVersion
    _intermediate_versions: list[DerivativeRelationVersion]

    def __post_init__(self) -> None:
        if not self._intermediate_versions or not self._inital_version:
            raise RelationWithoutLastVersionError

        for version, next_version in pairwise(self):
            if next(version.number) != next_version.number:
                raise NotIncrementedRelationVersionError

    def __len__(self) -> int:
        return len(self._intermediate_versions) + 1

    def __iter__(self) -> Iterator[RelationVersion]:
        yield self._inital_version
        yield from self._intermediate_versions

    def number(self) -> Number:
        return self._number

    def inital_version(self) -> InitialRelationVersion | None:
        return self._inital_version

    def intermediate_versions(
        self,
    ) -> Sequence[DerivativeRelationVersion]:
        return self._intermediate_versions

    def last_version(self) -> RelationVersion:
        return (
            self._intermediate_versions[-1]
            if self._intermediate_versions
            else self._inital_version
        )

    def last_version_id(self) -> RelationVersionID:
        return RelationVersionID(self._number, self.last_version().number)

    def recent_versions(
        self, current_version_number: Number
    ) -> Sequence[DerivativeRelationVersion]:
        if current_version_number < self._inital_version.number:
            return tuple()

        if current_version_number == self._inital_version.number:
            return self._intermediate_versions

        if current_version_number >= self._intermediate_versions[-1].number:
            return tuple()

        if not self._intermediate_versions:
            return tuple()

        current_version_index = int(
            self._intermediate_versions[0].number
        ) - int(current_version_number)

        return self._intermediate_versions[current_version_index + 1 :]

    def migrate(
        self,
        new_version_schema: Schema,
        new_version_migration_id: UUID,
    ) -> None:
        last_version = DerivativeRelationVersion(
            next(self.last_version().number),
            new_version_schema,
            new_version_migration_id,
        )
        self._intermediate_versions.append(last_version)

    def remove_old_versions(self, count: int) -> None:
        count_to_remove_intermediate_versions = count - 1
        count_to_remove_intermediate_versions = min(
            count_to_remove_intermediate_versions, len(self) - 1
        )
        del self._intermediate_versions[:count_to_remove_intermediate_versions]

        version_to_make_inital = self._intermediate_versions[0]

        self._inital_version = InitialRelationVersion(
            version_to_make_inital.number,
            version_to_make_inital.schema,
        )
        del self._intermediate_versions[0]

    @classmethod
    def new(cls, id: Number, schema: Schema) -> "Relation":
        return Relation(
            id,
            InitialRelationVersion(Number(0), schema),
            list(),
        )
