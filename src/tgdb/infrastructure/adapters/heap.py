from asyncio import gather
from dataclasses import dataclass

from in_memory_db import InMemoryDb

from tgdb.application.ports.heap import Heap
from tgdb.entities.message import Message
from tgdb.entities.row import (
    DeletedRow,
    MutatedRow,
    NewRow,
    Row,
    RowEffect,
)
from tgdb.entities.transaction import TransactionEffect
from tgdb.infrastructure.heap_row_encoding import encoded_heap_row
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.lazy_message_map import LazyMessageMap
from tgdb.infrastructure.telethon.mapping import message


@dataclass(frozen=True, unsafe_hash=False)
class InMemoryHeap(Heap):
    _db: InMemoryDb[Row]

    async def map(self, effect: TransactionEffect) -> None:
        for row_effect in effect:
            prevous_row = self._db.select_one(
                lambda row: row.id == row_effect.row_id
            )

            match row_effect, prevous_row:
                case DeletedRow(), Row():
                    self._db.remove(prevous_row)

                case NewRow(new_row), _:
                    self._db.insert(new_row)

                case MutatedRow(new_row), Row():
                    self._db.remove(prevous_row)
                    self._db.insert(new_row)

                case _: ...

    async def map_as_duplicate(self, effect: TransactionEffect) -> None:
        for row_effect in effect:
            prevous_row = self._db.select_one(
                lambda row: row.id == row_effect.row_id
            )

            match row_effect, prevous_row:
                case DeletedRow(row_id), Row():
                    self._db.remove(prevous_row)

                case NewRow(new_row) | MutatedRow(new_row), Row():
                    self._db.remove(prevous_row)
                    self._db.insert(new_row)

                case _: ...


@dataclass(frozen=True)
class InTelegramHeap(Heap):
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _pool_to_edit: TelegramClientPool
    _pool_to_delete: TelegramClientPool
    _heap_id: int
    _message_map: LazyMessageMap

    async def map(self, transaction_effect: TransactionEffect) -> None:
        await gather(*(
            self._map_row_effect(row_effect, is_duplicate=False)
            for row_effect in transaction_effect
        ))

    async def map_as_duplicate(
        self, transaction_effect: TransactionEffect
    ) -> None:
        await gather(*(
            self._map_row_effect(row_effect, is_duplicate=True)
            for row_effect in transaction_effect
        ))

    async def _map_row_effect(
        self, row_effect: RowEffect, *, is_duplicate: bool
    ) -> None:
        match row_effect:
            case NewRow():
                await self._insert(row_effect, is_duplicate)
            case MutatedRow():
                await self._update(row_effect)
            case DeletedRow():
                await self._delete(row_effect)

    async def _insert(self, row_effect: NewRow, is_duplicate: bool) -> None:
        if is_duplicate:
            message_ = await self._message_map[row_effect.row.id]

            if message_ is not None:
                return

        tg_new_message = await self._pool_to_insert().send_message(
            self._heap_id, encoded_heap_row(row_effect.row)
        )
        self._message_map[row_effect.row.id] = message(tg_new_message)

    async def _update(self, row_effect: MutatedRow) -> None:
        message = await self._message(row_effect)

        if message is None:
            return

        await self._pool_to_edit(message.author_id).edit_message(
            self._heap_id, message.id, encoded_heap_row(row_effect.row)
        )

    async def _delete(self, row_effect: DeletedRow) -> None:
        message = await self._message(row_effect)

        if message is None:
            return

        await self._pool_to_delete().delete_messages(
            self._heap_id, [message.id]
        )

    async def _message(
        self, row_effect: MutatedRow | DeletedRow
    ) -> Message | None:
        if row_effect.message is not None:
            return row_effect.message

        match row_effect:
            case MutatedRow():
                row_id = row_effect.row.id
            case DeletedRow():
                row_id = row_effect.row_id

        return await self._message_map[row_id]
