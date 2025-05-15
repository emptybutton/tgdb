from asyncio import gather
from collections.abc import AsyncIterator
from dataclasses import dataclass
from itertools import chain
from typing import cast

from telethon.hints import TotalList
from telethon.types import Message

from tgdb.entities.row import (
    Row,
    RowAttribute,
    RowSchema,
    decoded_row,
    encoded_row,
    query_text,
)
from tgdb.lazy_map import LazyMap
from tgdb.telethon.client_pool import TelegramClientPool
from tgdb.telethon.lazy_message_map import lazy_message_map
from tgdb.entities.transaction_effect import (
    RowInTransactionEffect,
)


@dataclass
class InTelegramRowHeap:
    pool_to_insert: TelegramClientPool
    pool_to_select: TelegramClientPool
    pool_to_edit: TelegramClientPool
    pool_to_delete: TelegramClientPool
    heap_id: int
    _message_map: LazyMap[Row, Message]

    def __post_init__(self) -> None:
        self._message_map = lazy_message_map(self.heap_id, self.pool_to_select)

    async def insert(self, row: Row) -> None:
        message = await self.pool_to_insert().send_message(
            self.heap_id, encoded_row(row)
        )
        self._message_map[row] = message

    async def row(
        self,
        schema: RowSchema,
        attribute_number: int,
        attribute: RowAttribute | None = None,
    ) -> Row | None:
        query = query_text(schema, attribute_number, attribute)

        messages = cast(TotalList, await self.pool_to_select().get_messages(
            self.heap_id, search=query, limit=1
        ))

        if not messages:
            return None

        message = messages[0]
        row = decoded_row(schema, message.message)

        self._message_map[row] = message

        return row

    async def rows(
        self,
        schema: RowSchema,
        attribute_number: int,
        attribute: RowAttribute | None = None,
    ) -> AsyncIterator[Row]:
        query = query_text(schema, attribute_number, attribute)

        messages = self.pool_to_select().iter_messages(
            self.heap_id, search=query, reverse=True
        )

        async for message in messages:
            row = decoded_row(schema, message.message)
            self._message_map[row] = message

            yield row

    async def update(self, row: Row) -> None:
        message = await self._message_map[row]

        if message is None:
            raise ValueError

        await self.pool_to_edit.edit(message, encoded_row(row))

    async def delete(self, row: Row) -> None:
        message = await self._message_map[row]

        if message is None:
            return

        await self.pool_to_delete().delete_messages(self.heap_id, message)

    async def execute(self, effect: RowInTransactionEffect) -> None:
        await gather(*chain(
            map(self.insert, effect.new_values),
            map(self.update, effect.mutated_values),
            map(self.delete, effect.dead_values),
        ))
