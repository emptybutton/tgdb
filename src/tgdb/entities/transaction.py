from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.mark import Mark
from tgdb.entities.operator import IntermediateOperator
from tgdb.entities.row import (
    DeletedRow,
    MutatedRow,
    NewRow,
    RowAttribute,
    RowEffect,
)


type TransactionEffect = Sequence[RowEffect]


@dataclass(frozen=True)
class TransactionConflict:
    marks: frozenset[Mark]


@dataclass(frozen=True)
class TransactionOkCommit:
    transaction_id: UUID
    effect: TransactionEffect
    time: LogicTime


@dataclass(frozen=True)
class TransactionFailedCommit:
    transaction_id: UUID
    conflict: TransactionConflict | None


type TransactionCommit = TransactionOkCommit | TransactionFailedCommit


@dataclass
class Transaction:
    id: UUID
    _beginning: LogicTime | None
    _effect: list[RowEffect]
    _is_readonly: bool
    _row_ids: set[RowAttribute]
    _marks: set[Mark]
    _concurrent_transactions: list["Transaction"]
    _transactions_with_possible_conflict: list["Transaction"]

    def add_operator(self, effect: IntermediateOperator) -> None:
        match effect:
            case NewRow() | MutatedRow() | DeletedRow() as row_effect:
                self._effect.append(row_effect)
                self._row_ids.add(row_effect.row.id)

                if self._is_readonly:
                    self._is_readonly = isinstance(row_effect, NewRow)

            case Mark() as mark:
                self._marks.add(mark)
                self._is_readonly = False

    def beginning(self) -> LogicTime | None:
        return self._beginning

    def rollback(self) -> None:
        self._die()

    def commit(self, current_time: LogicTime) -> TransactionCommit:
        conflict = self._conflict()

        if conflict is not None:
            self._die()
            return TransactionFailedCommit(self.id, conflict)

        is_conflictable = not self._is_readonly

        if is_conflictable:
            for transaction in self._concurrent_transactions:
                transaction._transactions_with_possible_conflict.append(self)

        self._die()
        return TransactionOkCommit(
            transaction_id=self.id,
            effect=self._effect,
            time=current_time,
        )

    @classmethod
    def start(
        cls,
        transaction_id: UUID,
        active_transactions: Iterable["Transaction"],
        current_time: LogicTime,
    ) -> "Transaction":
        transasction = Transaction(
            id=transaction_id,
            _beginning=current_time,
            _is_readonly=True,
            _effect=list(),
            _row_ids=set(),
            _marks=set(),
            _concurrent_transactions=list(active_transactions),
            _transactions_with_possible_conflict=list(),
        )

        for active_transaction in active_transactions:
            active_transaction._concurrent_transactions.append(transasction)

        return transasction

    def _die(self) -> None:
        self._concurrent_transactions.clear()

    def _conflict(self) -> TransactionConflict | None:
        for transaction in self._transactions_with_possible_conflict:
            conflict_marks = frozenset(self._marks & transaction._marks)
            conflict_row_ids = self._row_ids & transaction._row_ids

            if conflict_marks or conflict_row_ids:
                return TransactionConflict(marks=conflict_marks)

        return None
