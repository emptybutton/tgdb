from abc import ABC
from collections.abc import Awaitable

from tgdb.entities.logic_time import LogicTime


class LogicClock(ABC, Awaitable[LogicTime]): ...
