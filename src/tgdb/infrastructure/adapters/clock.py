from collections.abc import Generator
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Any

from tgdb.application.common.ports.clock import Clock
from tgdb.entities.time.logic_time import LogicTime


@dataclass
class InMemoryClock(Clock):
    _time_counter: int = 0

    def __await__(self) -> Generator[Any, Any, LogicTime]:
        return self._time().__await__()

    async def _time(self) -> LogicTime:
        self._time_counter += 1
        return self._time_counter


@dataclass(frozen=True)
class PerfCounterClock(Clock):
    def __await__(self) -> Generator[Any, Any, LogicTime]:
        return self._time().__await__()

    async def _time(self) -> LogicTime:
        return perf_counter_ns()
