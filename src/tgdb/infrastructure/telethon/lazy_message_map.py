from typing import cast

from telethon.hints import TotalList
from telethon.types import Message

from tgdb.lazy_map import LazyMap
from tgdb.telethon.client_pool import TelegramClientPool
from tgdb.entities.row import Row, query_text


def lazy_message_map(
    chat_id: int, pool: TelegramClientPool
) -> LazyMap[Row, Message]:
    async def message_of_row(row: Row) -> Message | None:
        query = query_text(row.schema, 0, row[0])

        messages = cast(TotalList, await pool.get_messages(
            chat_id, search=query, limit=1
        ))

        return messages[0] if messages else None

    return LazyMap(message_of_row)
