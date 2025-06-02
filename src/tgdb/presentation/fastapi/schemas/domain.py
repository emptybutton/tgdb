from abc import ABC, abstractmethod
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, PositiveInt

from tgdb.application.common.operator import (
    DeletedTupleOperator,
    MutatedTupleOperator,
    NewTupleOperator,
    Operator,
)
from tgdb.entities.horizon.claim import Claim
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.domain import (
    IntDomain,
    SetDomain,
    StrDomain,
    TypeDomain,
)
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.schema import Schema
from tgdb.entities.relation.tuple import TID, Tuple
from tgdb.presentation.fastapi.schemas.encoding import EncodingSchema


class IntDomainSchema(EncodingSchema[IntDomain]):
    type: Literal["int"] = "int"
    min: int
    max: int
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> IntDomain:
        return IntDomain(self.min, self.max, self.is_nonable)


class StrDomainSchema(EncodingSchema[StrDomain]):
    type: Literal["str"] = "str"
    max_len: int = Field(alias="maxLen")
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> StrDomain:
        return StrDomain(self.max_len, self.is_nonable)


class BoolDomainSchema(EncodingSchema[TypeDomain]):
    type: Literal["bool"] = "bool"
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> TypeDomain:
        return TypeDomain(bool, self.is_nonable)


class TimeDomainSchema(EncodingSchema[TypeDomain]):
    type: Literal["time"] = "time"
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> TypeDomain:
        return TypeDomain(datetime, self.is_nonable)


class UuidDomainSchema(EncodingSchema[TypeDomain]):
    type: Literal["uuid"] = "uuid"
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> TypeDomain:
        return TypeDomain(UUID, self.is_nonable)


class IntSetDomainSchema(EncodingSchema[SetDomain[int]]):
    type: Literal["intSet"] = "intSet"
    ints: tuple[int, ...]
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> SetDomain[int]:
        return SetDomain(self.ints, int, self.is_nonable)


class StrSetDomainSchema(EncodingSchema[SetDomain[str]]):
    type: Literal["strSet"] = "strSet"
    strs: tuple[str, ...]
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> SetDomain[str]:
        return SetDomain(self.strs, str, self.is_nonable)


class TimeSetDomainSchema(EncodingSchema[SetDomain[datetime]]):
    type: Literal["timeSet"] = "timeSet"
    times: tuple[datetime, ...]
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> SetDomain[datetime]:
        return SetDomain(self.times, datetime, self.is_nonable)


class UuidSetDomainSchema(EncodingSchema[SetDomain[UUID]]):
    type: Literal["uuidSet"] = "uuidSet"
    uuids: tuple[UUID, ...]
    is_nonable: bool = Field(alias="IsNonable")

    def decoded(self) -> SetDomain[UUID]:
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
