from asyncio import gather

from tgdb.infrastructure.telethon.client_pool import TelegramClientPool


async def chat_id_range(
    chat_id: int,
    pool_to_select: TelegramClientPool,
) -> range:
    min_id_messages, max_id_messages = await gather((
        pool_to_select().get_messages(chat_id, limit=1, reverse=True, min_id=1),
        pool_to_select().get_messages(chat_id, limit=1),
    ))

    if not min_id_messages or not max_id_messages:
        return range(1, 1)

    min_id = min_id_messages[0].id
    max_id = min_id_messages[0].id

    return range(min_id, max_id + 1)
