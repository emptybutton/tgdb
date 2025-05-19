from collections import deque
from collections.abc import Iterator, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from types import TracebackType
from typing import Self

from telethon import TelegramClient


@dataclass(init=False)
class TelegramClientPool(AbstractAsyncContextManager["TelegramClientPool"]):
    _clients: deque[TelegramClient]
    _client_by_id: dict[int, TelegramClient]

    def __init__(self, clients: Sequence[TelegramClient]) -> None:
        self._clients = deque(clients, len(clients))
        self._client_by_id = dict()

    async def __aenter__(self) -> Self:
        for client in self._clients:
            client_info = await client.get_me(input_peer=True)
            client_id = client_info.user_id

            self._client_by_id[client_id] = client

        return self

    async def __aexit__(
        self,
        _: type[BaseException] | None,
        __: BaseException | None,
        ___: TracebackType | None,
    ) -> None:
        return

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
