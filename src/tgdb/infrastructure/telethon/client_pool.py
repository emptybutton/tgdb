from asyncio import gather
from collections import deque
from collections.abc import Iterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Self, cast
from warnings import filterwarnings

from telethon import TelegramClient
from telethon.types import InputPeerUser

from tgdb.infrastructure.telethon.string_session_without_entites import (
    StringSessionWithoutEntites,
)


@dataclass(frozen=True, unsafe_hash=False)
class TelegramClientPool(AbstractAsyncContextManager["TelegramClientPool"]):
    _clients: deque[TelegramClient]

    _client_by_id: dict[int, TelegramClient] = field(
        init=False,
        default_factory=dict,
    )

    async def __aenter__(self) -> Self:
        await gather(
            *(
                client.__aenter__()  # type: ignore[no-untyped-call]
                for client in self._clients
            ),
        )

        for client in self._clients:
            client_info = cast(
                InputPeerUser,
                await client.get_me(input_peer=True),
            )
            client_id = client_info.user_id

            self._client_by_id[client_id] = client

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await gather(
            *(
                client.__aexit__(error_type, error, traceback)  # type: ignore[no-untyped-call]
                for client in self._clients
            ),
        )

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
    farm_file_path: Path,
    app_api_id: int,
    app_api_hash: str,
) -> TelegramClientPool:
    with farm_file_path.open() as farm_file:
        return TelegramClientPool(
            deque(
                pool_client(session_token, app_api_id, app_api_hash)
                for session_token in map(_clean_line, farm_file)
                if session_token
            ),
        )


def pool_client(
    session_token: str,
    app_api_id: int,
    app_api_hash: str,
) -> TelegramClient:
    filterwarnings(
        "ignore",
        category=UserWarning,
        module="telethon.client.updates",
        message=(
            "in-memory entities exceed entity_cache_limit after flushing;"
            " consider setting a larger limit"
        ),
    )

    return TelegramClient(
        StringSessionWithoutEntites(session_token),
        app_api_id,
        app_api_hash,
        entity_cache_limit=0,
    )


def _clean_line(line: str) -> str:
    return line.strip("\n ")
