from typing import Literal

from pydantic import BaseModel

from tgdb.entities.relation.domain import (
    BoolDomain,
    DatetimeDomain,
    Domain,
    IntDomain,
    SetDomain,
    StrDomain,
    UuidDomain,
)
from tgdb.entities.relation.scalar import Scalar


class EncodableIntDomain(BaseModel):
    type: Literal["int"] = "int"
    min: int
    max: int
    is_nonable: bool

    @classmethod
    def of(cls, domain: "IntDomain") -> "EncodableIntDomain":
        return cls(min=domain.min, max=domain.max, is_nonable=domain.is_nonable)

    def entity(self) -> "IntDomain":
        return IntDomain(self.min, self.max, self.is_nonable)


class EncodableStrDomain(BaseModel):
    type: Literal["str"] = "str"
    max_len: int
    is_nonable: bool

    @classmethod
    def of(cls, domain: "StrDomain") -> "EncodableStrDomain":
        return cls(
            max_len=domain.max_len,
            is_nonable=domain.is_nonable,
        )

    def entity(self) -> "StrDomain":
        return StrDomain(self.max_len, self.is_nonable)


class EncodableBoolDomain(BaseModel):
    type: Literal["bool"] = "bool"
    is_nonable: bool

    @classmethod
    def of(cls, domain: BoolDomain) -> "EncodableBoolDomain":
        return cls(is_nonable=domain.is_nonable)

    def entity(self) -> BoolDomain:
        return BoolDomain(self.is_nonable)


class EncodableTimeDomain(BaseModel):
    type: Literal["time"] = "time"
    is_nonable: bool

    @classmethod
    def of(cls, domain: DatetimeDomain) -> "EncodableTimeDomain":
        return cls(is_nonable=domain.is_nonable)

    def entity(self) -> DatetimeDomain:
        return DatetimeDomain(self.is_nonable)


class EncodableUuidDomain(BaseModel):
    type: Literal["uuid"] = "uuid"
    is_nonable: bool

    @classmethod
    def of(cls, domain: UuidDomain) -> "EncodableUuidDomain":
        return cls(is_nonable=domain.is_nonable)

    def entity(self) -> UuidDomain:
        return UuidDomain(self.is_nonable)


class EncodableSetDomain(BaseModel):
    type: Literal["set"] = "set"
    scalars: tuple[Scalar, ...]

    @classmethod
    def of(cls, domain: SetDomain) -> "EncodableSetDomain":
        return cls(scalars=domain)

    def entity(self) -> SetDomain:
        return self.scalars


type EncodableDomain = (
    EncodableBoolDomain
    | EncodableIntDomain
    | EncodableStrDomain
    | EncodableTimeDomain
    | EncodableUuidDomain
    | EncodableSetDomain
)


def encodable_domain(domain: Domain) -> EncodableDomain:
    match domain:
        case IntDomain():
            return EncodableIntDomain.of(domain)

        case StrDomain():
            return EncodableStrDomain.of(domain)

        case BoolDomain():
            return EncodableBoolDomain.of(domain)

        case UuidDomain():
            return EncodableUuidDomain.of(domain)

        case DatetimeDomain():
            return EncodableTimeDomain.of(domain)

        case tuple():
            return EncodableSetDomain.of(domain)
