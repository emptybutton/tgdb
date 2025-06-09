from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar, cast

from telethon.hints import TotalList

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID, Tuple
from tgdb.entities.tools.assert_ import assert_
from tgdb.infrastructure.heap_tuple_encoding import HeapTupleEncoding
from tgdb.infrastructure.lazy_map import LazyMap
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.index import (
    MessageIndex,
    TupleIndex,
    message_index,
)


@dataclass(frozen=True)
class UnacceptableTupleError(Exception):
    encoded_tuple_len: int


@dataclass(frozen=True, unsafe_hash=False)
class InTelegramHeap:
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _pool_to_edit: TelegramClientPool
    _pool_to_delete: TelegramClientPool
    _heap_id: int
    _encoded_tuple_max_len: int
    _index_map: LazyMap[TupleIndex, MessageIndex | None]

    _page_len: ClassVar = 4000

    @staticmethod
    def encoded_tuple_max_len(page_max_fullness: float) -> int:
        page_max_fullness = max(0, page_max_fullness)
        page_max_fullness = min(page_max_fullness, 1)

        return int(page_max_fullness * InTelegramHeap._page_len)

    def __post_init__(self) -> None:
        assert_(
            self._encoded_tuple_max_len <= InTelegramHeap._page_len,
            ValueError,
        )

    def tuple_max_len(self) -> int:
        return self._encoded_tuple_max_len

    def assert_can_accept_tuple(self, tuple_: Tuple) -> None:
        """
        :raises tgdb.infrastructure.telethon.in_telegram_heap.UnacceptableTupleError:
        """  # noqa: E501

        encoded_largest_tuple = HeapTupleEncoding.encoded_tuple(tuple_)

        if len(encoded_largest_tuple) > self._encoded_tuple_max_len:
            raise UnacceptableTupleError(len(encoded_largest_tuple))

    def assert_can_accept_tuples_of_relation(self, relation: Relation) -> None:
        """
        :raises tgdb.infrastructure.telethon.in_telegram_heap.UnacceptableTupleError:
        """  # noqa: E501

        schema = relation.last_version().schema
        schema_id = relation.last_version_schema_id()

        largest_tuple = HeapTupleEncoding.largest_tuple(schema, schema_id)

        self.assert_can_accept_tuple(largest_tuple)

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
            self._heap_id,
            search=search,
            reverse=True,
        )
        messages = cast(TotalList, messages)

        return tuple(
            HeapTupleEncoding.decoded_tuple(message.text)
            for message in messages
        )

    async def insert_idempotently(self, tuple_: Tuple) -> None:
        message_index_ = await self._index_map[self._heap_id, tuple_.tid]

        if message_index_ is not None:
            return

        new_message = await self._pool_to_insert().send_message(
            self._heap_id,
            HeapTupleEncoding.encoded_tuple(tuple_),
        )
        self._index_map[self._heap_id, tuple_.tid] = message_index(new_message)

    async def insert(self, tuple_: Tuple) -> None:
        new_message = await self._pool_to_insert().send_message(
            self._heap_id,
            HeapTupleEncoding.encoded_tuple(tuple_),
        )
        self._index_map[self._heap_id, tuple_.tid] = message_index(new_message)

    async def update(self, tuple_: Tuple) -> None:
        message_index = await self._index_map[self._heap_id, tuple_.tid]

        if message_index is None:
            return

        message_id, sender_id = message_index

        await self._pool_to_edit(sender_id).edit_message(
            self._heap_id,
            message_id,
            HeapTupleEncoding.encoded_tuple(tuple_),
        )

    async def delete_tuple_with_tid(self, tid: TID) -> None:
        message_index = await self._index_map[self._heap_id, tid]

        if message_index is None:
            return

        message_id, _ = message_index

        await self._pool_to_delete().delete_messages(
            self._heap_id,
            [message_id],
        )
