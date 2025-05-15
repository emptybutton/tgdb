import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from telethon.types import Message

from tgdb.telethon.client_pool import TelegramClientPool
from tgdb.telethon.in_telegram.primitive import (
    InTelegramPrimitive,
)


@dataclass(frozen=True)
class InTelegramMessageQueque:
    pool_to_push: TelegramClientPool
    pool_to_pull: TelegramClientPool
    in_tg_offset: InTelegramPrimitive[int]

    heap_id: int
    offset_message_chat_id: int
    offset_message_id: int
    seconds_to_wait_after_sync: int | float

    async def push(self, text: str, /) -> None:
        await self.pool_to_push().send_message(self.heap_id, text)

    async def top_message(self) -> Message | None:
        top_messages = await self.pool_to_pull().get_messages(
            self.heap_id, limit=1
        )

        if not top_messages:
            return None

        top_message = top_messages[0]

        if top_message.message is None:
            return None

        return top_message

    async def __aiter__(self) -> AsyncIterator[Message]:
        offset = await self.in_tg_offset

        while True:
            heap_messages_after_offset = self.pool_to_pull().iter_messages(
                self.heap_id, reverse=True, min_id=offset
            )

            async for heap_message in heap_messages_after_offset:
                yield heap_message
                offset = heap_message.id

            await asyncio.sleep(self.seconds_to_wait_after_sync)

    async def commit(self, message: Message) -> None:
        await self.in_tg_offset.set(message.id)
