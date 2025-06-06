from dataclasses import dataclass

from tgdb.application.common.ports.clock import Clock
from tgdb.application.common.ports.uuids import UUIDs
from tgdb.application.horizon.ports.shared_horizon import SharedHorizon
from tgdb.entities.horizon.transaction import (
    XID,
    IsolationLevel,
)


@dataclass(frozen=True)
class StartTransaction:
    uuids: UUIDs
    shared_horizon: SharedHorizon
    clock: Clock

    async def __call__(self, isolation_level: IsolationLevel) -> XID:
        time = await self.clock
        xid = await self.uuids.random_uuid()

        async with self.shared_horizon as horizon:
            return horizon.start_transaction(time, xid, isolation_level)
