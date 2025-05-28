from dataclasses import dataclass

from tgdb.application.ports.clock import Clock
from tgdb.application.ports.shared_horizon import SharedHorizon
from tgdb.application.ports.uuids import UUIDs
from tgdb.entities.horizon.transaction import XID


@dataclass(frozen=True)
class RollbackTransaction:
    uuids: UUIDs
    shared_horizon: SharedHorizon
    clock: Clock

    async def __call__(self, xid: XID) -> None:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        """

        async with self.shared_horizon as horizon:
            time = await self.clock
            horizon.rollback_transaction(time, xid)
