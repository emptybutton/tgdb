from asyncio import gather
from collections import deque
from collections.abc import Iterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Self, cast

from telethon import TelegramClient
from telethon.sessions.string import StringSession
from telethon.types import InputPeerUser


@dataclass(frozen=True, unsafe_hash=False)
class TelegramClientPool(AbstractAsyncContextManager["TelegramClientPool"]):
    _clients: deque[TelegramClient]

    _client_by_id: dict[int, TelegramClient] = field(
        init=False, default_factory=dict
    )

    async def __aenter__(self) -> Self:
        for client in self._clients:
            client_info = (
                cast(InputPeerUser, await client.get_me(input_peer=True))
            )
            client_id = client_info.user_id

            self._client_by_id[client_id] = client

        await gather(*(
            client.__aenter__()  # type: ignore[no-untyped-call]
            for client in self._clients
        ))

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await gather(*(
            client.__aexit__(error_type, error, traceback)  # type: ignore[no-untyped-call]
            for client in self._clients
        ))

    def __call__(self, client_id: int | None = None) -> TelegramClient:
        if client_id is None:
            client = self._clients.pop()
            self._clients.appendleft(client)

            return client

        client = self._client_by_id[client_id]

        self._clients.remove(client)
        self._clients.appendleft(client)

        return client

    def __iter__(self) -> Iterator[TelegramClient]:
        while True:
            yield self()


def loaded_client_pool_from_farm_file(
    farm_file_path: Path, app_api_id: int, app_api_hash: str
) -> TelegramClientPool:
    with farm_file_path.open() as farm_file:
        return TelegramClientPool(deque(
            TelegramClient(
                StringSession(session_token),
                app_api_id,
                app_api_hash,
                entity_cache=None,
            )
            for session_token in farm_file
            if session_token
        ))
