from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.entities.horizon.claim import Claim
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID


@dataclass(frozen=True)
class NewTupleOperator:
    relation_number: Number | None
    scalars: tuple[Scalar, ...] | None


@dataclass(frozen=True)
class MutatedTupleOperator:
    tid: TID | None
    relation_number: Number | None
    scalars: tuple[Scalar, ...] | None


@dataclass(frozen=True)
class DeletedTupleOperator:
    tid: TID | None


type Operator = (
    NewTupleOperator | MutatedTupleOperator | DeletedTupleOperator | Claim
)


class OperatorEncoding[EncodedOperatorsT](ABC):
    @abstractmethod
    async def decoded(
        self, operators: EncodedOperatorsT, /
    ) -> Sequence[Operator | None] | None: ...
