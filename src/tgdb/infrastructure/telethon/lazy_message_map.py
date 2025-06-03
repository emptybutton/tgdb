from typing import cast

from telethon.hints import TotalList
from telethon.tl.types import Message

from tgdb.entities.relation.tuple import TID
from tgdb.infrastructure.heap_tuple_encoding import HeapTupleEncoding
from tgdb.infrastructure.lazy_map import ExternalValue, LazyMap, NoExternalValue
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool


type LazyMessageMap = LazyMap[TID, Message]


def lazy_message_map(
    chat_id: int, pool: TelegramClientPool, computed_map_max_len: int
) -> LazyMessageMap:
    async def tuple_message(tid: TID) -> ExternalValue[Message]:
        search = HeapTupleEncoding.id_of_encoded_tuple_with_tid(tid)

        messages = cast(TotalList, await pool().get_messages(
            chat_id, search=search, limit=1
        ))

        if not messages:
            return NoExternalValue()

        return cast(Message, messages[0])

    return LazyMap(computed_map_max_len, tuple_message)
