from abc import ABC
from collections.abc import Awaitable
from contextlib import AbstractAsyncContextManager

from tgdb.entities.horizon.horizon import Horizon


class SharedHorizon(AbstractAsyncContextManager[Horizon], ABC): ...
