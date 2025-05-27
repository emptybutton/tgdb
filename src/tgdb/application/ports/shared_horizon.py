from abc import ABC
from collections.abc import Awaitable

from tgdb.entities.horizon.horizon import Horizon


class SharedHorizon(Awaitable[Horizon], ABC): ...
