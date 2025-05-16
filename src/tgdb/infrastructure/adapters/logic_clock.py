from collections.abc import Generator
from dataclasses import dataclass


@dataclass
class InMemoryLogicClock:
    _current_time: int = 0

    def __await__(self) -> Generator[None, None, int]:
        self._current_time += 1
        yield
        return self._current_time
