from dataclasses import dataclass
from types import TracebackType
from typing import ClassVar, Self

from in_memory_db import InMemoryDb
from pydantic import TypeAdapter

from tgdb.application.relation.ports.relations import (
    NoRelationError,
    NotUniqueRelationNumberError,
    Relations,
)
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation
from tgdb.infrastructure.pydantic.relation.relation import EncodableRelation
from tgdb.infrastructure.telethon.in_telegram_bytes import InTelegramBytes


@dataclass(frozen=True)
class InMemoryRelations(Relations):
    _db: InMemoryDb[Relation]

    async def relation(self, relation_number: Number) -> Relation:
        relation = self._db.select_one(
            lambda it: it.number() == relation_number,
        )
        if relation is None:
            raise NoRelationError

        return relation

    async def add(self, relation: Relation) -> None:
        selected_relation = self._db.select_one(
            lambda it: it.number() == relation.number(),
        )

        if selected_relation is not None:
            raise NotUniqueRelationNumberError

        self._db.insert(relation)


@dataclass
class InTelegramReplicableRelations(Relations):
    _in_tg_encoded_relations: InTelegramBytes
    _cached_relations: InMemoryDb[Relation]

    _adapter: ClassVar = TypeAdapter(tuple[EncodableRelation, ...])

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

    def cache(self) -> InMemoryDb[Relation]:
        return self._cached_relations

    async def relation(self, relation_number: Number) -> Relation:
        """
        :raises tgdb.application.relation.ports.relations.NoRelationError:
        """

        relation = self._cached_relations.select_one(
            lambda it: it.number() == relation_number,
        )

        if relation is None:
            raise NoRelationError

        return relation

    async def add(self, relation: Relation) -> None:
        """
        :raises tgdb.application.relation.ports.relations.NotUniqueRelationNumberError:
        """  # noqa: E501

        selected_relation = self._cached_relations.select_one(
            lambda it: it.number() == relation.number(),
        )

        if selected_relation is not None:
            raise NotUniqueRelationNumberError

        self._cached_relations.insert(relation)

        encodable_relations = tuple(
            map(EncodableRelation.of, self._cached_relations),
        )
        encoded_relations = self._adapter.dump_json(encodable_relations)
        await self._in_tg_encoded_relations.set(encoded_relations)

    async def _loaded_relations(self) -> tuple[Relation, ...]:
        encoded_relations = await self._in_tg_encoded_relations

        if encoded_relations is None:
            return tuple()

        encodable_relations = self._adapter.validate_json(encoded_relations)
        return tuple(it.entity() for it in encodable_relations)
