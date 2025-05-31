import pickle
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self, cast

from in_memory_db import InMemoryDb

from tgdb.application.common.ports.relations import (
    NoRelationError,
    NotUniqueRelationNumberError,
    Relations,
)
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation
from tgdb.infrastructure.telethon.in_telegram_bytes import InTelegramBytes


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

        return relation

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


@dataclass
class InTelegramReplicableRelations(Relations):
    _in_telegram_encoded_relations: InTelegramBytes
    _cached_relations: InMemoryDb[Relation] | None = field(
        init=False, default=None
    )

    async def __aenter__(self) -> Self:
        loaded_relations = await self._loaded_relations()
        self._cached_relations = loaded_relations

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def relation(self, relation_number: Number) -> Relation:
        """
        :raises tgdb.application.common.ports.relations.NoRelationError:
        """

        if self._cached_relations is None:
            raise ValueError

        relation = self._cached_relations.select_one(
            lambda it: it.number() == relation_number
        )

        if relation is None:
            raise NoRelationError(relation_number)

        return relation

    async def add(self, relation: Relation) -> None:
        """
        :raises tgdb.application.common.ports.relations.NotUniqueRelationNumberError:
        """  # noqa: E501

        if self._cached_relations is None:
            raise ValueError

        selected_relation = self._cached_relations.select_one(
            lambda it: it.number() == relation.number()
        )

        if selected_relation is not None:
            raise NotUniqueRelationNumberError(relation.number())

        self._cached_relations.insert(relation)

        encoded_cached_relations = pickle.dumps(self._cached_relations)
        await self._in_telegram_encoded_relations.set(encoded_cached_relations)

    async def _loaded_relations(self) -> InMemoryDb[Relation]:
        encoded_relations = await self._in_telegram_encoded_relations

        if encoded_relations is None:
            return InMemoryDb()

        relations = pickle.loads(encoded_relations)

        if relations is None:
            return InMemoryDb()
        if not isinstance(relations, InMemoryDb):
            raise TypeError(relations)

        for relation in relations:
            if not isinstance(relation, Relation):
                raise TypeError(relation)

        return cast(InMemoryDb[Relation], relations)
