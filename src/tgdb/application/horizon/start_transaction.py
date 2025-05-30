from dataclasses import dataclass

from tgdb.application.common.ports.clock import Clock
from tgdb.application.common.ports.shared_horizon import SharedHorizon
from tgdb.application.common.ports.uuids import UUIDs
from tgdb.entities.horizon.transaction import (
    IsolationLevel,
)


@dataclass(frozen=True)
class StartTransaction:
    uuids: UUIDs
    shared_horizon: SharedHorizon
    clock: Clock

    async def __call__(self, isolation_level: IsolationLevel) -> None:
        time = await self.clock
        xid = await self.uuids.random_uuid()

        async with self.shared_horizon as horizon:
            horizon.start_transaction(time, xid, isolation_level)
