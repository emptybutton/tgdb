from collections.abc import Awaitable, Generator
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, cast

from telethon.hints import TotalList

from tgdb.infrastructure.telethon.client_pool import TelegramClientPool


@dataclass
class InTelegramBytes(Awaitable[bytes | None]):
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _chat_id: int

    _cached_stored_bytes: bytes | None = field(init=False, default=None)

    def __await__(self) -> Generator[Any, Any, bytes | None]:
        return self._get().__await__()

    async def set(self, bytes: bytes) -> None:
        client = self._pool_to_insert()

        await client.send_message(
            self._chat_id, file=bytes
        )
        self._cached_stored_bytes = bytes

    async def _get(self) -> bytes | None:
        if self._cached_stored_bytes is not None:
            return self._cached_stored_bytes

        await self._refresh()

        return self._cached_stored_bytes

    async def _refresh(self) -> None:
        messages = await self._pool_to_select().get_messages(
            self._chat_id, limit=1
        )
        messages = cast(TotalList, messages)

        if not messages:
            return

        last_message = messages[-1]

        with BytesIO() as stream:
            await self._pool_to_select().download_file(last_message, stream)
            stored_bytes = stream.getvalue()

        self._cached_stored_bytes = stored_bytes
