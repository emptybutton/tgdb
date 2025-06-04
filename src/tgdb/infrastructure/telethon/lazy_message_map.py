from typing import cast

from telethon.hints import TotalList
from telethon.tl.types import Message

from tgdb.entities.relation.tuple import TID
from tgdb.infrastructure.heap_tuple_encoding import HeapTupleEncoding
from tgdb.infrastructure.lazy_map import ExternalValue, LazyMap, NoExternalValue
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool


type ChatID = int
type LazyMessageMap = LazyMap[tuple[ChatID, TID], Message]


def lazy_message_map(
    pool: TelegramClientPool, cache_map_max_len: int
) -> LazyMessageMap:
    async def tuple_message(
        chat_id_and_tid: tuple[ChatID, TID]
    ) -> ExternalValue[Message]:
        chat_id, tid = chat_id_and_tid

        search = HeapTupleEncoding.id_of_encoded_tuple_with_tid(tid)

        messages = cast(TotalList, await pool().get_messages(
            chat_id, search=search, limit=1
        ))

        if not messages:
            return NoExternalValue()

        return cast(Message, messages[0])

    return LazyMap(cache_map_max_len, tuple_message)
