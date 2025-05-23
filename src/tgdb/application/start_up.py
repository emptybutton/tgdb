from dataclasses import dataclass

from tgdb.application.errors.common import ActiveNodeError
from tgdb.application.ports.shared_horizon import SharedHorizon
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.transaction_horizon import create_transaction_horizon


@dataclass(frozen=True)
class StartUp:
    shared_horizon: SharedHorizon

    async def __call__(
        self,
        horizon_max_width: LogicTime | None,
        horizon_max_height: int | None,
    ) -> None:
        """
        :raises tgdb.application.errors.common.ActiveNodeError:
        """

        async with self.shared_horizon as horizon:
            if horizon is not None:
                raise ActiveNodeError

            new_horizon = create_transaction_horizon(
                horizon_max_width, horizon_max_height
            )

            await self.shared_horizon.set(new_horizon)
