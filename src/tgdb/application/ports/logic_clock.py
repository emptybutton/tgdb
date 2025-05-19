from abc import ABC, abstractmethod
from collections.abc import Awaitable, Sequence

from tgdb.entities.logic_time import LogicTime


type Chronology = Sequence[LogicTime]


class LogicClock(ABC, Awaitable[LogicTime]):
    @abstractmethod
    async def chronology(self, len: int, /) -> Chronology: ...
