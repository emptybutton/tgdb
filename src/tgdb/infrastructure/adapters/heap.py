from asyncio import gather
from dataclasses import dataclass, field
from itertools import groupby

from tgdb.application.ports.heap import Heap
from tgdb.entities.message import Message
from tgdb.entities.row import DeletedRow, MutatedRow, NewRow, Row, RowEffect
from tgdb.entities.transaction import TransactionEffect
from tgdb.infrastructure.row_encoding import encoded_row
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.lazy_message_map import LazyMessageMap
from tgdb.infrastructure.telethon.mapping import message


@dataclass(frozen=True, unsafe_hash=False)
class InMemoryHeap(Heap):
    _rows: set[Row] = field(default_factory=set)

    async def map(self, effect: TransactionEffect) -> None:
        for row_operator_effect in effect:
            match row_operator_effect, row_operator_effect.row in self._rows:
                case NewRow(row) | MutatedRow(row), _:
                    self._rows.add(row)
                case DeletedRow(row), True:
                    self._rows.remove(row)
                case _: ...


@dataclass(frozen=True)
class TelethonHeap(Heap):
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _pool_to_edit: TelegramClientPool
    _pool_to_delete: TelegramClientPool
    _heap_id: int
    _message_map: LazyMessageMap

    async def map(self, transaction_effect: TransactionEffect) -> None:
        row_effects_by_row_id = groupby(
            transaction_effect, key=lambda it: it.row.id
        )

        await gather(*(
            self._map_row_effects(tuple(row_effects))
            for _, row_effects in row_effects_by_row_id
        ))

    async def _map_row_effects(self, effect_part: TransactionEffect) -> None:
        await gather(*map(self._map_row_effect, effect_part))

    async def _map_row_effect(self, row_effect: RowEffect) -> None:
        match row_effect:
            case NewRow():
                await self._insert(row_effect)
            case MutatedRow():
                await self._update(row_effect)
            case DeletedRow():
                await self._delete(row_effect)

    async def _insert(self, row_effect: NewRow) -> None:
        telethon_message = await self._pool_to_insert().send_message(
            self._heap_id, encoded_row(row_effect.row)
        )
        self._message_map[row_effect.row.schema, row_effect.row.id] = (
            message(telethon_message)
        )

    async def _update(self, row_effect: MutatedRow) -> None:
        message = await self._message(row_effect)

        if message is None:
            return

        await self._pool_to_edit(message.author_id).edit_message(
            self._heap_id, message.id, encoded_row(row_effect.row)
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

        return await self._message_map[row_effect.row.schema, row_effect.row.id]
