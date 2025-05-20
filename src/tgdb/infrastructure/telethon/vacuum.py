from asyncio import gather
from dataclasses import dataclass
from itertools import batched
from math import ceil

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
            self.pool_to_delete().delete_message(chat_id, id_batch_to_delete)
            for id_batch_to_delete in id_batches_to_delete
        ))
