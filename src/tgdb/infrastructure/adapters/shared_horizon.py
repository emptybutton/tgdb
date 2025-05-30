from types import TracebackType

from tgdb.application.common.ports.shared_horizon import SharedHorizon
from tgdb.entities.horizon.horizon import Horizon


class InMemorySharedHorizon(SharedHorizon):
    _horizon: Horizon

    async def __aenter__(self) -> Horizon:
        return self._horizon

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None: ...
