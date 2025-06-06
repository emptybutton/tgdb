from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from tgdb.entities.horizon.horizon import (
    InvalidTransactionStateError,
    NoTransactionError,
)
from tgdb.entities.horizon.transaction import XID


@dataclass(frozen=True)
class Notification:
    error: NoTransactionError | InvalidTransactionStateError | None


class Channel(ABC):
    @abstractmethod
    async def publish(
        self,
        ok_commit_xids: Sequence[XID],
        error_commit_map: Mapping[
            XID, NoTransactionError | InvalidTransactionStateError
        ],
        /,
    ) -> None: ...

    @abstractmethod
    async def wait(self, xid: XID, /) -> Notification: ...
