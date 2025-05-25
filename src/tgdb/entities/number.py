from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Self, overload
from uuid import UUID


class NegativeNumberError(Exception): ...


@dataclass(frozen=True)
class Number:
    int: int

    def __post_init__(self) -> None:
        if self.int < 0:
            raise NegativeNumberError

    def __int__(self) -> "int":
        return self.int

    def __next__(self) -> "Number":
        return Number(self.int + 1)

    def __lt__(self, other: "Number") -> bool:
        return self.int < other.int
