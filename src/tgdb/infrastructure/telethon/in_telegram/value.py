from abc import ABC, abstractmethod
from collections.abc import Generator
from dataclasses import dataclass

from telethon.types import Message

from tgdb.telethon.client_pool import TelegramClientPool


@dataclass(frozen=True)
class InTelegramValue[ValueT](ABC):
    pool_to_insert: TelegramClientPool
    pool_to_select: TelegramClientPool
    pool_to_delete: TelegramClientPool
    pointer_chat_id: int

    async def set(self, value: ValueT, /) -> None:
        await self.pool_to_insert().send_message(
            self.pointer_chat_id,
            self._encoded(value),
        )

    async def get(self) -> ValueT:
        pointer_message = await self._pointer_message()

        return self._decoded(pointer_message.message)

    async def refresh(self) -> None:
        pointer_message = await self._pointer_message()
        ids_to_delete = list(range(pointer_message.id))
        client = self.pool_to_delete()

        await client.delete_message(self.pointer_chat_id, ids_to_delete)

    @abstractmethod
    def _encoded(self, value: ValueT, /) -> str: ...

    @abstractmethod
    def _decoded(self, encoded_value: str, /) -> ValueT: ...

    async def _pointer_message(self) -> Message:
        pointer_messages = await self.pool_to_select().get_messages(
            self.pointer_chat_id, limit=1
        )

        return pointer_messages[0]
