from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from tgdb.entities.relation.scalar import Scalar


@dataclass(frozen=True)
class IntDomain:
    min: int
    max: int
    is_nonable: bool

    def __contains__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return self.is_nonable

        return isinstance(scalar, int) and self.min <= scalar <= self.max


@dataclass(frozen=True)
class StrDomain:
    max_len: int
    is_nonable: bool

    def __contains__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return self.is_nonable

        return isinstance(scalar, str) and len(scalar) <= self.max_len


@dataclass(frozen=True)
class _TypeDomain:
    is_nonable: bool
    _type: type = field(init=False)

    def __contains__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return self.is_nonable

        return scalar is True or scalar is False


@dataclass(frozen=True)
class BoolDomain(_TypeDomain):
    _type = bool


@dataclass(frozen=True)
class DatetimeDomain(_TypeDomain):
    _type = datetime


@dataclass(frozen=True)
class UuidDomain(_TypeDomain):
    _type = UUID


type SetDomain = tuple[Scalar, ...]


type Domain = (
    IntDomain | StrDomain | BoolDomain | DatetimeDomain | UuidDomain | SetDomain
)
