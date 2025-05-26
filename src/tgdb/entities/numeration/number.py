from dataclasses import dataclass


class NegativeNumberError(Exception): ...


@dataclass(frozen=True)
class Number:
    """
    :raises tgdb.entities.numeration.number.NegativeNumberError:
    """

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

    def __le__(self, other: "Number") -> bool:
        return self.int <= other.int
