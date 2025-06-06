from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from tgdb.application.horizon.ports.channel import Channel, Notification
from tgdb.entities.horizon.horizon import (
    InvalidTransactionStateError,
    NoTransactionError,
)
from tgdb.entities.horizon.transaction import XID
from tgdb.infrastructure.async_map import AsyncMap


@dataclass(frozen=True)
class AsyncMapChannel(Channel):
    _async_map: AsyncMap[
        XID, NoTransactionError | InvalidTransactionStateError | None
    ]

    async def publish(
        self,
        ok_commit_xids: Sequence[XID],
        error_commit_map: Mapping[
            XID, NoTransactionError | InvalidTransactionStateError
        ],
    ) -> None:
        for ok_commit_xid in ok_commit_xids:
            self._async_map[ok_commit_xid] = None
            del self._async_map[ok_commit_xid]

        for error_commit_xid, error in error_commit_map.items():
            self._async_map[error_commit_xid] = error
            del self._async_map[error_commit_xid]

    async def wait(self, xid: XID) -> Notification:
        notification_error = await self._async_map[xid]
        return Notification(notification_error)
