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
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID


class InsertOperatorSchema(BaseModel):
    action: Literal["insert"] = "insert"
    relation_number: PositiveInt = Field(alias="relationNumber")
    scalars: tuple[Scalar, ...]

    def decoded(self) -> NewTupleOperator:
        return NewTupleOperator(Number(self.relation_number), self.scalars)


class UpdateOperatorSchema(BaseModel):
    action: Literal["update"] = "update"
    relation_number: PositiveInt = Field(alias="relationNumber")
    tid: TID
    scalars: tuple[Scalar, ...]

    def decoded(self) -> MutatedTupleOperator:
        return MutatedTupleOperator(
            self.tid, Number(self.relation_number), self.scalars
        )


class DeleteOperatorSchema(BaseModel):
    action: Literal["delete"] = "delete"
    tid: TID

    def decoded(self) -> DeletedTupleOperator:
        return DeletedTupleOperator(self.tid)


class ClaimOperatorSchema(BaseModel):
    action: Literal["claim"] = "claim"
    id: UUID
    object: str

    def decoded(self) -> Claim:
        return Claim(self.id, self.object)


type OperatorSchema = (
    InsertOperatorSchema
    | UpdateOperatorSchema
    | DeleteOperatorSchema
    | ClaimOperatorSchema
)


class OperatorListSchema(BaseModel):
    operators: tuple[OperatorSchema, ...]

    def decoded(self) -> tuple[Operator, ...]:
        return tuple(operator.decoded() for operator in self.operators)
