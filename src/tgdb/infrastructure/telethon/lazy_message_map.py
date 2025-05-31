from typing import cast

from telethon.hints import TotalList
from telethon.tl.types import Message

from tgdb.entities.relation.tuple import TID
from tgdb.infrastructure.lazy_map import LazyMap
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool


type LazyMessageMap = LazyMap[TID, Message]


def lazy_message_map(
    chat_id: int, pool: TelegramClientPool, computed_map_max_len: int
) -> LazyMessageMap:
    async def message_of_row(tid: TID) -> Message | None:
        query = row_with_id_query(tid)

        messages = cast(TotalList, await pool().get_messages(
            chat_id, search=query, limit=1
        ))

        if not messages:
            return None

        return messages[0]

    return LazyMap(computed_map_max_len, message_of_row)
