from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.row import Row


@dataclass(frozen=True)
class NewRow:
    row: Row


@dataclass(frozen=True)
class MutatedRow:
    row: Row


@dataclass(frozen=True)
class DeletedRow:
    row: Row


type RowOperatorEffect = NewRow | MutatedRow | DeletedRow


class TransactionState(StrEnum):
    started = "started"
    committed = "committed"
    rollbacked = "rollbacked"


@dataclass(frozen=True)
class Mark:
    mark_id: UUID
    key: str


type IntermediateOperatorEffect = RowOperatorEffect | Mark
type OperatorEffect = IntermediateOperatorEffect | TransactionState


@dataclass(frozen=True)
class Operator:
    effect: OperatorEffect
    transaction_id: UUID


@dataclass(frozen=True)
class AppliedOperator:
    effect: OperatorEffect
    transaction_id: UUID
    time: LogicTime


def not_applied_operator(operator: AppliedOperator) -> Operator:
    return Operator(operator.effect, operator.transaction_id)


def applied_operator(
    operator: Operator, current_time: LogicTime
) -> AppliedOperator:
    return AppliedOperator(
        operator.effect,
        operator.transaction_id,
        current_time,
    )
