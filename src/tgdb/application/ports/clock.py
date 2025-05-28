from abc import ABC
from collections.abc import Awaitable

from tgdb.entities.time.logic_time import LogicTime


class Clock(ABC, Awaitable[LogicTime]): ...
