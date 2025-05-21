from dataclasses import dataclass

from tgdb.application.ports.logic_clock import LogicClock
from tgdb.entities.logic_time import LogicTime


@dataclass
class InMemoryLogicClock(LogicClock):
    _time_counter: int = 0

    async def time(self) -> LogicTime:
        self._time_counter += 1
        return self._time_counter
