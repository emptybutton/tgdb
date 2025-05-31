from asyncio import TaskGroup
from collections.abc import Awaitable, Generator
from dataclasses import dataclass, field
from io import BytesIO
from types import TracebackType
from typing import Any, Self, cast

from telethon.hints import TotalList

from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.vacuum import AutoVacuum


@dataclass
class InTelegramBytes(Awaitable[bytes | None]):
    _pool_to_insert: TelegramClientPool
    _pool_to_select: TelegramClientPool
    _chat_id: int
    _auto_vacuum: AutoVacuum

    _tasks: TaskGroup = field(init=False, default_factory=TaskGroup)
    _cached_stored_bytes: bytes | None = field(init=False, default=None)

    async def __aenter__(self) -> Self:
        await self._tasks.__aenter__()
        self._tasks.create_task(self._auto_vacuum(self._chat_id))

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return await self._tasks.__aexit__(error_type, error, traceback)

    def __await__(self) -> Generator[Any, Any, bytes | None]:
        return self._get().__await__()

    async def set(self, bytes: bytes) -> None:
        client = self._pool_to_insert()

        last_message = await client.send_message(
            self._chat_id, file=bytes
        )

        self._auto_vacuum.update_horizon(last_message.id)
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

        self._auto_vacuum.update_horizon(last_message.id)
        self._cached_stored_bytes = stored_bytes
