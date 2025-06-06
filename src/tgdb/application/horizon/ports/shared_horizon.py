from abc import ABC
from contextlib import AbstractAsyncContextManager

from tgdb.entities.horizon.horizon import Horizon


class SharedHorizon(AbstractAsyncContextManager[Horizon], ABC): ...
