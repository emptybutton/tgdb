from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, Field

from tgdb.entities.relation.domain import (
    Domain,
    IntDomain,
    SetDomain,
    StrDomain,
    TypeDomain,
)


class IntDomainSchema(BaseModel):
    type: Literal["int"] = "int"
    min: int
    max: int
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: "IntDomain") -> "IntDomainSchema":
        return cls(
            min=domain.min,
            max=domain.max,
            IsNonable=domain.is_nonable()
        )

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
            IsNonable=domain.is_nonable()
        )

    def decoded(self) -> "StrDomain":
        return StrDomain(self.max_len, self.is_nonable)


class BoolDomainSchema(BaseModel):
    type: Literal["bool"] = "bool"
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: TypeDomain[bool]) -> "BoolDomainSchema":
        return cls(IsNonable=domain.is_nonable())

    def decoded(self) -> TypeDomain[bool]:
        return TypeDomain(bool, self.is_nonable)


class TimeDomainSchema(BaseModel):
    type: Literal["time"] = "time"
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: TypeDomain[datetime]) -> "TimeDomainSchema":
        return cls(IsNonable=domain.is_nonable())

    def decoded(self) -> TypeDomain[datetime]:
        return TypeDomain(datetime, self.is_nonable)


class UuidDomainSchema(BaseModel):
    type: Literal["uuid"] = "uuid"
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: TypeDomain[UUID]) -> "UuidDomainSchema":
        return cls(IsNonable=domain.is_nonable())

    def decoded(self) -> TypeDomain[UUID]:
        return TypeDomain(UUID, self.is_nonable)


class IntSetDomainSchema(BaseModel):
    type: Literal["intSet"] = "intSet"
    ints: tuple[int, ...]
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: SetDomain[int]) -> "IntSetDomainSchema":
        return cls(
            ints=domain.values,
            IsNonable=domain.is_nonable()
        )

    def decoded(self) -> SetDomain[int]:
        return SetDomain(self.ints, int, self.is_nonable)


class StrSetDomainSchema(BaseModel):
    type: Literal["strSet"] = "strSet"
    strs: tuple[str, ...]
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: SetDomain[str]) -> "StrSetDomainSchema":
        return cls(
            strs=domain.values,
            IsNonable=domain.is_nonable()
        )

    def decoded(self) -> SetDomain[str]:
        return SetDomain(self.strs, str, self.is_nonable)


class TimeSetDomainSchema(BaseModel):
    type: Literal["timeSet"] = "timeSet"
    times: tuple[datetime, ...]
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: SetDomain[datetime]) -> "TimeSetDomainSchema":
        return cls(
            times=domain.values,
            IsNonable=domain.is_nonable()
        )

    def decoded(self) -> SetDomain[datetime]:
        return SetDomain(self.times, datetime, self.is_nonable)


class UuidSetDomainSchema(BaseModel):
    type: Literal["uuidSet"] = "uuidSet"
    uuids: tuple[UUID, ...]
    is_nonable: bool = Field(alias="IsNonable")

    @classmethod
    def of(cls, domain: SetDomain[UUID]) -> "UuidSetDomainSchema":
        return cls(
            uuids=domain.values,
            IsNonable=domain.is_nonable()
        )

    def decoded(self) -> "SetDomain[UUID]":
        return SetDomain(self.uuids, UUID, self.is_nonable)


type DomainSchema = (
    BoolDomainSchema
    | IntDomainSchema
    | StrDomainSchema
    | TimeDomainSchema
    | UuidDomainSchema
    | IntSetDomainSchema
    | StrSetDomainSchema
    | TimeSetDomainSchema
    | UuidSetDomainSchema
)


def domain_schema(domain: Domain) -> DomainSchema:
    match domain:
        case IntDomain():
            return IntDomainSchema.of(domain)

        case StrDomain():
            return StrDomainSchema.of(domain)

        case SetDomain():
            return _set_domain_schema(domain)

        case TypeDomain():
            return _type_domain_schema(domain)


def _set_domain_schema(
    domain: (
        SetDomain[int] | SetDomain[str] | SetDomain[datetime] | SetDomain[UUID]
    )
) -> (
    IntSetDomainSchema
    | StrSetDomainSchema
    | TimeSetDomainSchema
    | UuidSetDomainSchema
):
    if domain.type() is int:
        domain = cast(SetDomain[int], domain)
        return IntSetDomainSchema.of(domain)

    if domain.type() is str:
        domain = cast(SetDomain[str], domain)
        return StrSetDomainSchema.of(domain)

    if domain.type() is datetime:
        domain = cast(SetDomain[datetime], domain)
        return TimeSetDomainSchema.of(domain)

    if domain.type() is UUID:
        domain = cast(SetDomain[UUID], domain)
        return UuidSetDomainSchema.of(domain)

    raise ValueError


def _type_domain_schema(
    domain: TypeDomain[bool] | TypeDomain[datetime] | TypeDomain[UUID]
) -> (
    BoolDomainSchema
    | TimeDomainSchema
    | UuidDomainSchema
):
    if domain.type() is bool:
        domain = cast(TypeDomain[bool], domain)
        return BoolDomainSchema.of(domain)

    if domain.type() is datetime:
        domain = cast(TypeDomain[datetime], domain)
        return TimeDomainSchema.of(domain)

    if domain.type() is UUID:
        domain = cast(TypeDomain[UUID], domain)
        return UuidDomainSchema.of(domain)

    raise ValueError
