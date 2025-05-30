from asyncio import gather
from collections.abc import Sequence
from dataclasses import dataclass

from in_memory_db import InMemoryDb

from tgdb.application.common.ports.heap import Heap
from tgdb.entities.message import Message
from tgdb.entities.row import (
    DeletedRow,
    MutatedRow,
    NewRow,
    Row,
)
from tgdb.entities.transaction import TransactionEffect, TransactionScalarEffect
from tgdb.infrastructure.heap_row_encoding import encoded_heap_row
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.lazy_message_map import LazyMessageMap
from tgdb.infrastructure.telethon.mapping import message


@dataclass(frozen=True, unsafe_hash=False)
class InMemoryHeap(Heap):
    _db: InMemoryDb[Row]

    async def map(self, effects: Sequence[TransactionEffect]) -> None:
        await gather(*map(self._map_one, effects))

    async def map_as_duplicate(
        self, effects: Sequence[TransactionEffect]
    ) -> None:
        await gather(*map(self._map_one_as_duplicate, effects))

    async def _map_one(self, effect: TransactionEffect) -> None:
        for row_with_effect in effect:
            prevous_row = self._db.select_one(
                lambda row: row.id == row_with_effect.id  # noqa: B023
            )

            match row_with_effect, prevous_row:
                case DeletedRow(), Row():
                    self._db.remove(prevous_row)

                case NewRow(next_row), _:
                    self._db.insert(next_row)

                case MutatedRow(next_row), Row():
                    self._db.remove(prevous_row)
                    self._db.insert(next_row)

                case _: ...

    async def _map_one_as_duplicate(self, effect: TransactionEffect) -> None:
        for row_with_effect in effect:
            prevous_row = self._db.select_one(
                lambda row: row.id == row_with_effect.id  # noqa: B023
            )

            match row_with_effect, prevous_row:
                case DeletedRow(), Row():
                    self._db.remove(prevous_row)

                case NewRow(next_row) | MutatedRow(next_row), Row():
                    self._db.remove(prevous_row)
                    self._db.insert(next_row)

                case _: ...


@dataclass(frozen=True)
class InTelegramHeap(Heap):
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _pool_to_edit: TelegramClientPool
    _pool_to_delete: TelegramClientPool
    _heap_id: int
    _message_map: LazyMessageMap

    async def map(
        self, transaction_effects: Sequence[TransactionEffect]
    ) -> None:
        await gather(*(
            self._map_scalar_effect(row_effect, is_duplicate=False)
            for transaction_effect in transaction_effects
            for row_effect in transaction_effect
        ))

    async def map_as_duplicate(
        self, transaction_effects: Sequence[TransactionEffect]
    ) -> None:
        await gather(*(
            self._map_scalar_effect(row_effect, is_duplicate=True)
            for transaction_effect in transaction_effects
            for row_effect in transaction_effect
        ))

    async def _map_scalar_effect(
        self, scalar_effect: TransactionScalarEffect, *, is_duplicate: bool
    ) -> None:
        match scalar_effect:
            case NewRow() as row:
                await self._insert(row, is_duplicate)
            case MutatedRow() as row:
                await self._update(row)
            case DeletedRow() as row:
                await self._delete(row)

    async def _insert(self, new_row: NewRow, is_duplicate: bool) -> None:
        if is_duplicate:
            message_ = await self._message_map[new_row.row.id]

            if message_ is not None:
                return

        tg_new_message = await self._pool_to_insert().send_message(
            self._heap_id, encoded_heap_row(new_row.row)
        )
        self._message_map[new_row.row.id] = message(tg_new_message)  # type: ignore[arg-type]

    async def _update(self, mutated_row: MutatedRow) -> None:
        message = await self._message(mutated_row)

        if message is None:
            return

        await self._pool_to_edit(message.author_id).edit_message(
            self._heap_id, message.id, encoded_heap_row(mutated_row.row)
        )

    async def _delete(self, deleted_row: DeletedRow) -> None:
        message = await self._message(deleted_row)

        if message is None:
            return

        await self._pool_to_delete().delete_messages(
            self._heap_id, [message.id]
        )

    async def _message(
        self, row_with_effect: MutatedRow | DeletedRow
    ) -> Message | None:
        if row_with_effect.message is not None:
            return row_with_effect.message

        return await self._message_map[row_with_effect.id]
