from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, overload
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

    def __add__(self, other: "Number | int") -> "Number":
        return Number(self.int + int(other))

    def __radd__(self, other: "int") -> "Number":
        return Number(self.int + other)
