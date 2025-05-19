from dataclasses import dataclass

from tgdb.application.ports.logic_clock import Chronology, LogicClock
from tgdb.entities.logic_time import LogicTime


@dataclass
class InMemoryLogicClock(LogicClock):
    _time_counter: int = 0

    async def chronology(self, len_: int, /) -> Chronology:
        return tuple(self._time() for _ in range(len_))

    def _time(self) -> LogicTime:
        self._time_counter += 1
        return self._time_counter
