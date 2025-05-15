from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.row import RowEffect
from tgdb.entities.transaction_mark import TransactionMark


type OperatorValue = RowEffect | TransactionMark


@dataclass(frozen=True)
class Operator:
    value: OperatorValue
    transaction_id: UUID


@dataclass(frozen=True)
class AppliedOperator:
    value: OperatorValue
    transaction_id: UUID
    time: LogicTime


def not_applied_operator(operator: AppliedOperator) -> Operator:
    return Operator(operator.value, operator.transaction_id)


def applied_operator(
    operator: Operator, current_time: LogicTime
) -> AppliedOperator:
    return AppliedOperator(
        operator.value,
        operator.transaction_id,
        current_time,
    )
