from dataclasses import dataclass

from tgdb.entities.logic_time import LogicTime


@dataclass
class LogicClock:
    _time_counter: int = 0

    def time(self) -> LogicTime:
        self._time_counter += 1
        return self._time_counter
