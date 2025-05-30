from dataclasses import dataclass

from tgdb.entities.horizon.claim import Claim
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID


@dataclass(frozen=True)
class NewTupleOperator:
    relation_number: Number
    scalars: tuple[Scalar, ...]


@dataclass(frozen=True)
class MutatedTupleOperator:
    tid: TID
    relation_number: Number
    scalars: tuple[Scalar, ...]


@dataclass(frozen=True)
class DeletedTupleOperator:
    tid: TID


type Operator = (
    NewTupleOperator | MutatedTupleOperator | DeletedTupleOperator | Claim
)
