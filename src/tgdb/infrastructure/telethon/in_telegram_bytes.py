from collections.abc import Awaitable, Generator
from dataclasses import dataclass
from io import BytesIO
from typing import Any, cast

from telethon.hints import TotalList

from tgdb.infrastructure.telethon.client_pool import TelegramClientPool


@dataclass
class InTelegramBytes(Awaitable[bytes | None]):
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _chat_id: int

    def __await__(self) -> Generator[Any, Any, bytes | None]:
        return self._get().__await__()

    async def set(self, bytes_: bytes) -> None:
        await self._pool_to_insert().send_message(self._chat_id, file=bytes_)

    async def _get(self) -> bytes | None:
        messages = await self._pool_to_select().get_messages(
            self._chat_id,
            min_id=1,
        )
        messages = cast(TotalList, messages)

        if not messages:
            return None

        last_message = messages[-1]

        with BytesIO() as stream:
            await self._pool_to_select().download_file(last_message, stream)
            return stream.getvalue()
