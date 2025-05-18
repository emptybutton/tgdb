from abc import ABC, abstractmethod
from collections.abc import Sequence

from tgdb.entities.logic_time import LogicTime


class LogicClock(ABC):
    @abstractmethod
    async def times(self, count: int) -> Sequence[LogicTime]: ...

    @abstractmethod
    async def time(self) -> LogicTime: ...
