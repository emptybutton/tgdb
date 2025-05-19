from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.mark import Mark
from tgdb.entities.row import RowEffect


type IntermediateOperator = RowEffect | Mark


@dataclass(frozen=True)
class StartOperator:
    transaction_id: UUID


@dataclass(frozen=True)
class CommitOperator:
    transaction_id: UUID
    operators: Sequence[IntermediateOperator]


@dataclass(frozen=True)
class RollbackOperator:
    transaction_id: UUID


type Operator = StartOperator | CommitOperator | RollbackOperator


@dataclass(frozen=True)
class AppliedOperator:
    operator: Operator
    time: LogicTime
