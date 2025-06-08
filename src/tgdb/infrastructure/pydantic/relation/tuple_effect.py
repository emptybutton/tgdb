from typing import Literal

from pydantic import BaseModel

from tgdb.entities.relation.tuple import TID
from tgdb.entities.relation.tuple_effect import (
    DeletedTuple,
    MigratedTuple,
    MutatedTuple,
    NewTuple,
)
from tgdb.infrastructure.pydantic.relation.tuple import EncodableTuple


class EncodableNewTuple(BaseModel):
    type: Literal["new"] = "new"
    tuple: EncodableTuple

    def entity(self) -> NewTuple:
        return NewTuple(self.tuple.entity())

    @classmethod
    def of(cls, effect: NewTuple) -> "EncodableNewTuple":
        return cls(tuple=EncodableTuple.of(effect.tuple))


class EncodableMutatedTuple(BaseModel):
    type: Literal["mutated"] = "mutated"
    tuple: EncodableTuple

    def entity(self) -> MutatedTuple:
        return MutatedTuple(self.tuple.entity())

    @classmethod
    def of(cls, effect: MutatedTuple) -> "EncodableMutatedTuple":
        return cls(tuple=EncodableTuple.of(effect.tuple))


class EncodableMigratedTuple(BaseModel):
    type: Literal["migrated"] = "migrated"
    tuple: EncodableTuple

    def entity(self) -> MigratedTuple:
        return MigratedTuple(self.tuple.entity())

    @classmethod
    def of(cls, effect: MigratedTuple) -> "EncodableMigratedTuple":
        return cls(tuple=EncodableTuple.of(effect.tuple))


class EncodableDeletedTuple(BaseModel):
    tid: TID

    def entity(self) -> DeletedTuple:
        return DeletedTuple(tid=self.tid)

    @classmethod
    def of(cls, effect: DeletedTuple) -> "EncodableDeletedTuple":
        return cls(tid=effect.tid)
