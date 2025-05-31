from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

from telethon.hints import TotalList

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID, Tuple
from tgdb.infrastructure.heap_tuple_encoding import HeapTupleEncoding
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.lazy_message_map import LazyMessageMap


@dataclass(frozen=True, unsafe_hash=False)
class InTelegramHeap:
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _pool_to_edit: TelegramClientPool
    _pool_to_delete: TelegramClientPool
    _heap_id: int
    _message_map: LazyMessageMap

    async def tuples_with_attribute(
        self,
        relation_number: Number,
        attribute_number: Number,
        attribute_scalar: Scalar,
    ) -> Sequence[Tuple]:
        search = HeapTupleEncoding.id_of_encoded_tuple_with_attribute(
            int(relation_number),
            int(attribute_number),
            attribute_scalar,
        )

        messages = await self._pool_to_select().get_messages(
            self._heap_id, search=search, reverse=True
        )
        messages = cast(TotalList, messages)

        return tuple(
            HeapTupleEncoding.decoded_tuple(message.text)
            for message in messages
        )

    async def insert_idempotently(self, tuple: Tuple) -> None:
        message_ = await self._message_map[tuple.tid]

        if message_ is not None:
            return

        new_message = await self._pool_to_insert().send_message(
            self._heap_id, HeapTupleEncoding.encoded_tuple(tuple)
        )
        self._message_map[tuple.tid] = new_message

    async def insert(self, tuple: Tuple) -> None:
        new_message = await self._pool_to_insert().send_message(
            self._heap_id, HeapTupleEncoding.encoded_tuple(tuple)
        )
        self._message_map[tuple.tid] = new_message

    async def update(self, tuple: Tuple) -> None:
        message = await self._message_map[tuple.tid]

        if message is None:
            return

        await self._pool_to_edit(message.sender_id).edit_message(  # type: ignore[attr-defined]
            self._heap_id, message.id, HeapTupleEncoding.encoded_tuple(tuple)
        )

    async def delete_tuple_with_tid(self, tid: TID) -> None:
        message = await self._message_map[tid]

        if message is None:
            return

        await self._pool_to_delete().delete_messages(
            self._heap_id, [message.id]
        )
