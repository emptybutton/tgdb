from asyncio import gather, sleep
from dataclasses import dataclass
from itertools import batched
from math import ceil
from typing import NoReturn, cast

from telethon.hints import TotalList

from tgdb.infrastructure.telethon.client_pool import TelegramClientPool


@dataclass(frozen=True)
class Vacuum:
    pool_to_delete: TelegramClientPool
    max_workers: int

    def __post_init__(self) -> None:
        assert self.max_workers >= 1

    async def __call__(self, chat_id: int, start: int | None, end: int) -> None:
        if start is None:
            start = 0

        range_ = range(start, end)

        if not range_:
            return

        batch_size = ceil(len(range_) / self.max_workers)
        id_batches_to_delete = batched(range_, batch_size, strict=False)

        await gather(*(
            self.pool_to_delete().delete_messages(chat_id, id_batch_to_delete)
            for id_batch_to_delete in id_batches_to_delete
        ))


@dataclass
class AutoVacuum:
    _pool_to_select: TelegramClientPool
    _vacuum: Vacuum
    _seconds_between_vacuums: int | float
    _min_message_count_to_delete: int
    _horizon_start: int | None

    def __post_init__(self) -> None:
        assert self._seconds_between_vacuums > 0
        assert self._min_message_count_to_delete > 0

    def update_horizon(self, new_horizon_start: int) -> None:
        if self._horizon_start is None:
            self._horizon_start = new_horizon_start
        else:
            self._horizon_start = max(self._horizon_start, new_horizon_start)

    async def __call__(self, chat_id: int) -> NoReturn:
        start_messages = await self._pool_to_select().get_messages(
            chat_id, limit=1, reverse=True, min_id=1
        )
        start_messages = cast(TotalList, start_messages)
        if start_messages:
            cached_chat_min_id_to_delete = cast(int, start_messages[0].id)
        else:
            cached_chat_min_id_to_delete = 1

        while True:
            await sleep(self._seconds_between_vacuums)

            if self._horizon_start is None:
                continue

            if self._horizon_start == cached_chat_min_id_to_delete:
                continue

            assert self._horizon_start > cached_chat_min_id_to_delete

            message_count_to_delete = (
                self._horizon_start - cached_chat_min_id_to_delete
            )

            if message_count_to_delete < self._min_message_count_to_delete:
                continue

            chat_min_id_after_vacuum = self._horizon_start

            await self._vacuum(
                chat_id, cached_chat_min_id_to_delete, self._horizon_start
            )

            cached_chat_min_id_to_delete = chat_min_id_after_vacuum
