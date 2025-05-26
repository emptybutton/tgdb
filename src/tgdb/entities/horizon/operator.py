from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.horizon.effect import (
    Claim,
    ConflictableTransactionScalarEffect,
    DeletedTuple,
    MutatedTuple,
    NewTuple,
)
from tgdb.entities.horizon.transaction import TransactionIsolation
from tgdb.entities.time.logic_time import LogicTime


@dataclass(frozen=True)
class StartOperator:
    transaction_id: UUID
    transaction_isolation: TransactionIsolation


type IntermediateOperatorEffect = ConflictableTransactionScalarEffect


@dataclass(frozen=True)
class IntermediateOperator[
    EffectT: IntermediateOperatorEffect = IntermediateOperatorEffect
]:
    transaction_id: UUID
    effect: IntermediateOperatorEffect


@dataclass(frozen=True)
class RollbackOperator:
    transaction_id: UUID


@dataclass(frozen=True)
class CommitOperator:
    transaction_id: UUID
    operators: Sequence[
        IntermediateOperator[NewTuple | MutatedTuple | DeletedTuple | Claim]
    ]


type Operator = (
    StartOperator
    | IntermediateOperator
    | RollbackOperator
    | CommitOperator
)


@dataclass(frozen=True)
class AppliedOperator:
    operator: Operator
    time: LogicTime
