from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from tgdb.entities.relation.scalar import Scalar


@dataclass(frozen=True)
class IntDomain:
    min: int
    max: int
    _is_nonable: bool

    def type(self) -> type[int]:
        return int

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return self._is_nonable

        return isinstance(scalar, int) and self.min <= scalar <= self.max


@dataclass(frozen=True)
class StrDomain:
    max_len: int
    _is_nonable: bool

    def type(self) -> type[str]:
        return str

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return self._is_nonable

        return isinstance(scalar, str) and len(scalar) <= self.max_len


@dataclass(frozen=True)
class SetDomain[T: int | str | datetime | UUID]:
    values: tuple[T, ...]
    _type: type[T]
    _is_nonable: bool

    def type(self) -> type[T]:
        return self._type

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return self._is_nonable

        return scalar in self.values


@dataclass(frozen=True)
class TypeDomain[T: bool | datetime | UUID]:
    _type: type[T]
    _is_nonable: bool

    def type(self) -> type[T]:
        return self._type

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return self._is_nonable

        return isinstance(scalar, self._type)


type Domain = (
    IntDomain
    | StrDomain
    | SetDomain[int]
    | SetDomain[str]
    | SetDomain[datetime]
    | SetDomain[UUID]
    | TypeDomain[bool]
    | TypeDomain[datetime]
    | TypeDomain[UUID]
)
