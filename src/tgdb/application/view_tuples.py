from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.ports.buffer import Buffer
from tgdb.application.ports.channel import Channel
from tgdb.application.ports.clock import Clock
from tgdb.application.ports.migrations import Migrations
from tgdb.application.ports.queque import Queque
from tgdb.application.ports.shared_horizon import SharedHorizon
from tgdb.application.ports.uuids import UUIDs
from tgdb.entities.horizon.horizon import (
    InvalidTransactionStateError,
    NoTransactionError,
)
from tgdb.entities.horizon.transaction import (
    XID,
    IsolationLevel,
    PreparedCommit,
)


@dataclass(frozen=True)
class ViewTuples:
    uuids: UUIDs
    shared_horizon: SharedHorizon
    clock: Clock
    migrations: Migrations

    async def __call__(self, xid: XID, ) -> None:
        time = await self.clock
        xid = await self.uuids.random_uuid()

        async with self.shared_horizon as horizon:
            horizon.start_transaction(time, xid, isolation_level)
