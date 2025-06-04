import pickle
from dataclasses import dataclass
from types import TracebackType
from typing import Self

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
        relation = self._db.select_one(
            lambda it: it.number() == relation_number
        )
        if relation is None:
            raise NoRelationError

        return relation

    async def add(self, relation: Relation) -> None:
        selected_relation = self._db.select_one(
            lambda it: it.number() == relation.number()
        )

        if selected_relation is not None:
            raise NotUniqueRelationNumberError

        self._db.insert(relation)


@dataclass
class InTelegramReplicableRelations(Relations):
    _in_tg_encoded_relations: InTelegramBytes
    _cached_relations: InMemoryDb[Relation]

    async def __aenter__(self) -> Self:
        for loaded_relation in await self._loaded_relations():
            self._cached_relations.insert(loaded_relation)

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

        relation = self._cached_relations.select_one(
            lambda it: it.number() == relation_number
        )

        if relation is None:
            raise NoRelationError

        return relation

    async def add(self, relation: Relation) -> None:
        """
        :raises tgdb.application.common.ports.relations.NotUniqueRelationNumberError:
        """  # noqa: E501

        selected_relation = self._cached_relations.select_one(
            lambda it: it.number() == relation.number()
        )

        if selected_relation is not None:
            raise NotUniqueRelationNumberError

        self._cached_relations.insert(relation)

        encoded_cached_relations = pickle.dumps(tuple(self._cached_relations))
        await self._in_tg_encoded_relations.set(encoded_cached_relations)

    async def _loaded_relations(self) -> tuple[Relation, ...]:
        encoded_relations = await self._in_tg_encoded_relations

        if encoded_relations is None:
            return tuple()

        relations = pickle.loads(encoded_relations)

        if not isinstance(relations, tuple):
            raise TypeError(relations)

        for relation in relations:
            if not isinstance(relation, Relation):
                raise TypeError(relation)

        return relations
