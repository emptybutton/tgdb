from collections.abc import Sequence, Set
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.relation.tuple import Tuple, TupleID


@dataclass(frozen=True)
class ViewedTuple:
    id: TupleID

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        return effect


@dataclass(frozen=True)
class NewTuple:
    tuple: Tuple

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        match effect:
            case ViewedTuple():
                return self
            case MutatedTuple(tuple):
                return NewTuple(tuple)
            case NewTuple() | DeletedTuple():
                return effect

    @property
    def id(self) -> TupleID:
        return self.tuple.id


@dataclass(frozen=True)
class MutatedTuple:
    tuple: Tuple

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        match effect:
            case ViewedTuple():
                return self
            case NewTuple(tuple):
                return MutatedTuple(tuple)
            case MutatedTuple() | DeletedTuple():
                return effect

    @property
    def id(self) -> TupleID:
        return self.tuple.id


@dataclass(frozen=True)
class DeletedTuple:
    id: TupleID

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        match effect:
            case ViewedTuple() | MutatedTuple() | DeletedTuple():
                return self
            case NewTuple(tuple):
                return MutatedTuple(tuple)


type TupleEffect = NewTuple | ViewedTuple | MutatedTuple | DeletedTuple


@dataclass(frozen=True)
class Claim:
    id: UUID
    object: str


type ConflictableTransactionScalarEffect = TupleEffect | Claim
type ConflictableTransactionEffect = Sequence[TupleEffect | Claim]

type TransactionScalarEffect = NewTuple | MutatedTuple | DeletedTuple
type TransactionEffect = Set[TransactionScalarEffect]
