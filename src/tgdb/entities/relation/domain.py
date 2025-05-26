from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from tgdb.entities.relation.scalar import Scalar


class Domain(ABC):
    @abstractmethod
    def type(self) -> type[bool | int | str | datetime | UUID]: ...

    @abstractmethod
    def is_nonable(self) -> bool: ...

    @abstractmethod
    def __contains__(self, scalar: Scalar) -> bool: ...

    @abstractmethod
    def __eq__(self, other: object) -> bool: ...

    @abstractmethod
    def __hash__(self) -> int: ...


@dataclass(frozen=True)
class IntDomain(Domain):
    min: int
    max: int
    _is_nonable: bool

    def type(self) -> type[int]:
        return int

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, scalar: Scalar) -> bool:
        return isinstance(scalar, int) and self.min <= scalar <= self.max


@dataclass(frozen=True)
class StrDomain(Domain):
    max_len: int
    _is_nonable: bool

    def type(self) -> type[str]:
        return str

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, scalar: Scalar) -> bool:
        return isinstance(scalar, str) and len(scalar) <= self.max_len


@dataclass(frozen=True)
class SetDomain[T: bool | int | str | datetime | UUID](Domain):
    values: tuple[T, ...]
    _type: type[T]
    _is_nonable: bool

    def type(self) -> type[T]:  # type: ignore[override]
        return self._type

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, scalar: Scalar) -> bool:
        return scalar in self.values


@dataclass(frozen=True)
class TypeDomain(Domain):
    _type: type[bool | datetime | UUID]
    _is_nonable: bool

    def type(self) -> type[bool | datetime | UUID]:
        return self._type

    def is_nonable(self) -> bool:
        return self._is_nonable

    def __contains__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return self._is_nonable

        return isinstance(scalar, self._type)
