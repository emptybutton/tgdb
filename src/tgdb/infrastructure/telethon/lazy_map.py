from typing import cast

from telethon.hints import TotalList
from telethon.tl.types import Message

from tgdb.infrastructure.heap_tuple_encoding import HeapTupleEncoding
from tgdb.infrastructure.lazy_map import LazyMap
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.index import (
    MessageIndex,
    TupleIndex,
    message_index,
)


type MessageIndexLazyMap = LazyMap[TupleIndex, MessageIndex | None]


def message_index_lazy_map(
    pool: TelegramClientPool,
    cache_map_max_len: int,
) -> LazyMap[TupleIndex, MessageIndex | None]:
    async def tuple_message(tuple_index: TupleIndex) -> MessageIndex | None:
        chat_id, tid = tuple_index

        search = HeapTupleEncoding.id_of_encoded_tuple_with_tid(tid)
        messages = cast(
            TotalList,
            await pool().get_messages(chat_id, search=search, limit=1),
        )

        if not messages:
            return None

        message = cast(Message, messages[0])
        return message_index(message)

    return LazyMap(cache_map_max_len, tuple_message)
