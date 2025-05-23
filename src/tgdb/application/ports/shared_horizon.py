from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager

from tgdb.entities.transaction_horizon import TransactionHorizon


class SharedHorizon(
    AbstractAsyncContextManager[TransactionHorizon | None], ABC
):
    @abstractmethod
    async def set(self, horizon: TransactionHorizon | None) -> None: ...
