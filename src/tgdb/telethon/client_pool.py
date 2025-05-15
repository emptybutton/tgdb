from collections import deque
from collections.abc import Iterator, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from types import TracebackType
from typing import Self

from telethon import TelegramClient
from telethon.types import Message


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

    def __call__(self) -> TelegramClient:
        client = self._clients.pop()
        self._clients.appendleft(client)

        return client

    def __iter__(self) -> Iterator[TelegramClient]:
        while True:
            yield self()

    async def edit(self, message: Message, text: str) -> None:
        sender = self._client_by_id[message.from_id.user_id]
        self._clients.remove(sender)
        self._clients.appendleft(sender)

        await sender.edit_message(message, text)
