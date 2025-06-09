from asyncio import gather
from collections.abc import Sequence
from dataclasses import dataclass

from in_memory_db import InMemoryDb

from tgdb.application.relation.ports.tuples import (
    OversizedRelationSchemaError,
    Tuples,
)
from tgdb.entities.horizon.transaction import (
    TransactionEffect,
    TransactionScalarEffect,
)
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import Tuple
from tgdb.entities.relation.tuple_effect import (
    DeletedTuple,
    MigratedTuple,
    MutatedTuple,
    NewTuple,
)
from tgdb.infrastructure.telethon.in_telegram_heap import (
    InTelegramHeap,
    UnacceptableTupleError,
)


@dataclass(frozen=True, unsafe_hash=False)
class InMemoryTuples(Tuples):
    _db: InMemoryDb[Tuple]

    async def assert_can_accept_tuples(self, relation: Relation) -> None: ...

    async def tuples_with_attribute(
        self,
        relation_number: Number,
        attribute_number: Number,
        attribute_scalar: Scalar,
    ) -> Sequence[Tuple]:
        return self._db.select_many(
            lambda it: (
                it.relation_schema_id.relation_number == relation_number
                and len(it) >= int(attribute_number)
                and it[int(attribute_number)] == attribute_scalar
            ),
        )

    async def map(self, effects: Sequence[TransactionEffect]) -> None:
        await gather(*map(self._map_one, effects))

    async def map_idempotently(
        self,
        effects: Sequence[TransactionEffect],
    ) -> None:
        await gather(*map(self._map_one_idempotently, effects))

    async def _map_one(self, effect: TransactionEffect) -> None:
        for tuple_effect in effect:
            prevous_tuple = self._db.select_one(
                lambda it: it.tid == tuple_effect.tid,  # noqa: B023
            )

            match tuple_effect, prevous_tuple:
                case DeletedTuple(), Tuple():
                    self._db.remove(prevous_tuple)

                case NewTuple(next_tuple), _:
                    self._db.insert(next_tuple)

                case (
                    MutatedTuple(next_tuple) | MigratedTuple(next_tuple),
                    Tuple(),
                ):
                    self._db.remove(prevous_tuple)
                    self._db.insert(next_tuple)

                case _:
                    ...

    async def _map_one_idempotently(self, effect: TransactionEffect) -> None:
        for tuple_effect in effect:
            prevous_tuple = self._db.select_one(
                lambda it: it.tid == tuple_effect.tid,  # noqa: B023
            )

            match tuple_effect, prevous_tuple:
                case DeletedTuple(), Tuple():
                    self._db.remove(prevous_tuple)

                case NewTuple(next_tuple) | MutatedTuple(next_tuple), Tuple():
                    self._db.remove(prevous_tuple)
                    self._db.insert(next_tuple)

                case _:
                    ...


@dataclass(frozen=True)
class InTelegramHeapTuples(Tuples):
    _heap: InTelegramHeap

    async def assert_can_accept_tuples(self, relation: Relation) -> None:
        try:
            self._heap.assert_can_accept_tuples_of_relation(relation)
        except UnacceptableTupleError as error:
            raise OversizedRelationSchemaError(
                error.encoded_tuple_len,
                self._heap.tuple_max_len(),
            ) from error

    async def tuples_with_attribute(
        self,
        relation_number: Number,
        attribute_number: Number,
        attribute_scalar: Scalar,
    ) -> Sequence[Tuple]:
        return await self._heap.tuples_with_attribute(
            relation_number,
            attribute_number,
            attribute_scalar,
        )

    async def map(
        self,
        transaction_effects: Sequence[TransactionEffect],
    ) -> None:
        await gather(
            *(
                self._map_scalar_effect(tuple_effect, idempotently=False)
                for transaction_effect in transaction_effects
                for tuple_effect in transaction_effect
            ),
        )

    async def map_idempotently(
        self,
        transaction_effects: Sequence[TransactionEffect],
    ) -> None:
        await gather(
            *(
                self._map_scalar_effect(tuple_effect, idempotently=True)
                for transaction_effect in transaction_effects
                for tuple_effect in transaction_effect
            ),
        )

    async def _map_scalar_effect(
        self,
        scalar_effect: TransactionScalarEffect,
        idempotently: bool,  # noqa: FBT001
    ) -> None:
        match scalar_effect, idempotently:
            case NewTuple(tuple), False:
                await self._heap.insert(tuple)
            case NewTuple(tuple), True:
                await self._heap.insert_idempotently(tuple)
            case MutatedTuple(tuple) | MigratedTuple(tuple), _:
                await self._heap.update(tuple)
            case DeletedTuple(tid), _:
                await self._heap.delete_tuple_with_tid(tid)
