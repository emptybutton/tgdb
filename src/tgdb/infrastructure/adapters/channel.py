from asyncio import wait_for
from dataclasses import dataclass

from tgdb.application.horizon.ports.channel import Channel, Notification
from tgdb.entities.horizon.horizon import (
    NoTransactionError,
    TransactionNotCommittingError,
)
from tgdb.entities.horizon.transaction import XID
from tgdb.infrastructure.async_map import AsyncMap


@dataclass(frozen=True)
class AsyncMapChannel(Channel):
    _async_map: AsyncMap[
        XID,
        NoTransactionError | TransactionNotCommittingError | None,
    ]
    _timeout_seconds: int | float

    async def publish(
        self,
        xid: XID,
        notification: Notification,
    ) -> None:
        self._async_map[xid] = notification
        del self._async_map[xid]

    async def wait(self, xid: XID) -> Notification:
        return await wait_for(self._async_map[xid], self._timeout_seconds)
