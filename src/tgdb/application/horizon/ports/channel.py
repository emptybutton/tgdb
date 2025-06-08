from abc import ABC, abstractmethod

from tgdb.entities.horizon.horizon import (
    NoTransactionError,
    TransactionNotCommittingError,
)
from tgdb.entities.horizon.transaction import XID


type Notification = NoTransactionError | TransactionNotCommittingError | None


class Channel(ABC):
    @abstractmethod
    async def publish(
        self,
        xid: XID,
        notification: Notification,
        /,
    ) -> None: ...

    @abstractmethod
    async def wait(self, xid: XID, /) -> Notification: ...
