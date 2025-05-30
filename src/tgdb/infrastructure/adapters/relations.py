from abc import ABC, abstractmethod
from dataclasses import dataclass

from in_memory_db import InMemoryDb

from tgdb.application.common.ports.relations import (
    NoRelationError,
    NotUniqueRelationNumberError,
    Relations,
)
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation


@dataclass(frozen=True)
class InMemoryRelations(Relations):
    _db: InMemoryDb[Relation]

    async def relation(self, relation_number: Number) -> Relation:
        """
        :raises tgdb.application.common.ports.relations.NoRelationError:
        """

        relation = self._db.select_one(
            lambda it: it.number() == relation_number
        )

        if relation is None:
            raise NoRelationError(relation_number)

    async def add(self, relation: Relation) -> None:
        """
        :raises tgdb.application.common.ports.relations.NotUniqueRelationNumberError:
        """  # noqa: E501

        selected_relation = self._db.select_one(
            lambda it: it.number() == relation.number()
        )

        if selected_relation is not None:
            raise NotUniqueRelationNumberError(relation.number())

        self._db.insert(relation)


@dataclass(frozen=True)
class InMemoryRelations(Relations):
    _db: InMemoryDb[Relation]

    async def relation(self, relation_number: Number) -> Relation:
        """
        :raises tgdb.application.common.ports.relations.NoRelationError:
        """

        relation = self._db.select_one(
            lambda it: it.number() == relation_number
        )

        if relation is None:
            raise NoRelationError(relation_number)

    async def add(self, relation: Relation) -> None:
        """
        :raises tgdb.application.common.ports.relations.NotUniqueRelationNumberError:
        """  # noqa: E501

        selected_relation = self._db.select_one(
            lambda it: it.number() == relation.number()
        )

        if selected_relation is not None:
            raise NotUniqueRelationNumberError(relation.number())

        self._db.insert(relation)
