from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.mark import Mark
from tgdb.entities.row import DeletedRow, MutatedRow, NewRow, RowEffect
from tgdb.entities.transaction import TransactionIsolation


@dataclass(frozen=True)
class StartOperator:
    transaction_id: UUID
    transaction_isolation: TransactionIsolation


type IntermediateOperatorEffect = RowEffect | Mark


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
        IntermediateOperator[NewRow | MutatedRow | DeletedRow | Mark]
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
