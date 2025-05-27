from collections.abc import Awaitable

from tgdb.entities.time.logic_time import LogicTime


class Clock(Awaitable[LogicTime]): ...
