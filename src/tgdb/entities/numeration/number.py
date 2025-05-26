from dataclasses import dataclass


class NegativeNumberError(Exception): ...


@dataclass(frozen=True)
class Number:
    """
    :raises tgdb.entities.numeration.number.NegativeNumberError:
    """

    _int: int

    def __post_init__(self) -> None:
        if self._int < 0:
            raise NegativeNumberError

    def __int__(self) -> "int":
        return self._int

    def __next__(self) -> "Number":
        return Number(self._int + 1)

    def __lt__(self, other: "Number") -> bool:
        return self._int < other._int

    def __le__(self, other: "Number") -> bool:
        return self._int <= other._int
