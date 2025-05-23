from dataclasses import dataclass

from tgdb.application.errors.common import NotActiveNodeError
from tgdb.application.ports.shared_horizon import SharedHorizon


@dataclass(frozen=True)
class StartDown:
    shared_horizon: SharedHorizon

    async def __call__(self) -> None:
        """
        :raises tgdb.application.errors.common.NotActiveNodeError:
        """

        async with self.shared_horizon as horizon:
            if horizon is not None:
                raise NotActiveNodeError

            await self.shared_horizon.set(None)
