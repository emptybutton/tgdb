from typing import Annotated, Literal
from uuid import UUID

from annotated_types import Ge
from pydantic import BaseModel, Field

from tgdb.application.common.operator import (
    DeletedTupleOperator,
    MutatedTupleOperator,
    NewTupleOperator,
)
from tgdb.entities.horizon.claim import Claim
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID


class InsertOperatorSchema(BaseModel):
    action: Literal["insert"] = "insert"
    relation_number: Annotated[int, Ge(0)] = Field(alias="relationNumber")
    scalars: tuple[Scalar, ...]

    def decoded(self) -> NewTupleOperator:
        return NewTupleOperator(Number(self.relation_number), self.scalars)


class UpdateOperatorSchema(BaseModel):
    action: Literal["update"] = "update"
    relation_number: Annotated[int, Ge(0)] = Field(alias="relationNumber")
    tid: TID
    scalars: tuple[Scalar, ...]

    def decoded(self) -> MutatedTupleOperator:
        return MutatedTupleOperator(
            self.tid,
            Number(self.relation_number),
            self.scalars,
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
