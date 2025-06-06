from dataclasses import dataclass

from tgdb.application.common.ports.clock import Clock
from tgdb.application.common.ports.uuids import UUIDs
from tgdb.application.horizon.ports.shared_horizon import SharedHorizon
from tgdb.entities.horizon.transaction import XID


@dataclass(frozen=True)
class RollbackTransaction:
    uuids: UUIDs
    shared_horizon: SharedHorizon
    clock: Clock

    async def __call__(self, xid: XID) -> None:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.TransactionCommittingError:
        """

        time = await self.clock

        async with self.shared_horizon as horizon:
            horizon.rollback_transaction(time, xid)
