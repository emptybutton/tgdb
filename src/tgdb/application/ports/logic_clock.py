from abc import ABC, abstractmethod
from collections.abc import Sequence

from tgdb.entities.logic_time import LogicTime


type Chronology = Sequence[LogicTime]


class LogicClock(ABC):
    @abstractmethod
    async def chronology(self, len: int, /) -> Chronology: ...
