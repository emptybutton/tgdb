from abc import ABC, abstractmethod

from tgdb.entities.logic_time import LogicTime


class LogicClock(ABC):
    @abstractmethod
    async def time(self) -> LogicTime: ...
