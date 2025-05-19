from typing import cast

from telethon.hints import TotalList

from tgdb.entities.message import Message
from tgdb.entities.row import RowAttribute, Schema
from tgdb.infrastructure.lazy_map import LazyMap
from tgdb.infrastructure.row_encoding import encoded_attribute
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.mapping import message


type LazyMessageMap = LazyMap[tuple[Schema, RowAttribute], Message]


def lazy_message_map(
    chat_id: int, pool: TelegramClientPool, computed_map_max_len: int
) -> LazyMessageMap:
    async def message_of_row(
        schema_and_id: tuple[Schema, RowAttribute]
    ) -> Message | None:
        schema, id = schema_and_id
        query = encoded_attribute(schema, 0, id)

        messages = cast(TotalList, await pool().get_messages(
            chat_id, search=query, limit=1
        ))

        if not messages:
            return None

        telethon_message = messages[0]

        return message(telethon_message)

    return LazyMap(computed_map_max_len, message_of_row)
