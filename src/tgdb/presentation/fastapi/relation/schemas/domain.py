from typing import Literal

from pydantic import BaseModel, Field

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


class IntDomainSchema(BaseModel):
    type: Literal["int"] = "int"
    min: int
    max: int
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: "IntDomain") -> "IntDomainSchema":
        return cls(min=domain.min, max=domain.max, IsNonable=domain.is_nonable)

    def decoded(self) -> "IntDomain":
        return IntDomain(self.min, self.max, self.is_nonable)


class StrDomainSchema(BaseModel):
    type: Literal["str"] = "str"
    max_len: int = Field(alias="maxLen")
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: "StrDomain") -> "StrDomainSchema":
        return cls(
            maxLen=domain.max_len,
            IsNonable=domain.is_nonable,
        )

    def decoded(self) -> "StrDomain":
        return StrDomain(self.max_len, self.is_nonable)


class BoolDomainSchema(BaseModel):
    type: Literal["bool"] = "bool"
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: BoolDomain) -> "BoolDomainSchema":
        return cls(IsNonable=domain.is_nonable)

    def decoded(self) -> BoolDomain:
        return BoolDomain(self.is_nonable)


class TimeDomainSchema(BaseModel):
    type: Literal["time"] = "time"
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: DatetimeDomain) -> "TimeDomainSchema":
        return cls(IsNonable=domain.is_nonable)

    def decoded(self) -> DatetimeDomain:
        return DatetimeDomain(self.is_nonable)


class UuidDomainSchema(BaseModel):
    type: Literal["uuid"] = "uuid"
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: UuidDomain) -> "UuidDomainSchema":
        return cls(IsNonable=domain.is_nonable)

    def decoded(self) -> UuidDomain:
        return UuidDomain(self.is_nonable)


class SetDomainSchema(BaseModel):
    type: Literal["set"] = "set"
    scalars: tuple[Scalar, ...]

    @classmethod
    def of(cls, domain: SetDomain) -> "SetDomainSchema":
        return cls(scalars=domain)

    def decoded(self) -> SetDomain:
        return self.scalars


type DomainSchema = (
    BoolDomainSchema
    | IntDomainSchema
    | StrDomainSchema
    | TimeDomainSchema
    | UuidDomainSchema
    | SetDomainSchema
)


def domain_schema(domain: Domain) -> DomainSchema:
    match domain:
        case IntDomain():
            return IntDomainSchema.of(domain)

        case StrDomain():
            return StrDomainSchema.of(domain)

        case BoolDomain():
            return BoolDomainSchema.of(domain)

        case UuidDomain():
            return UuidDomainSchema.of(domain)

        case DatetimeDomain():
            return TimeDomainSchema.of(domain)

        case tuple():
            return SetDomainSchema.of(domain)
