from collections.abc import Iterable
from dataclasses import dataclass
from itertools import chain
from typing import Any, Never
from uuid import UUID

from effect import Effect, IdentifiedValueSet
from effect.state_transition import InvalidStateTransitionError

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.row import Row, RowEffect
from tgdb.entities.transaction_mark import (
    TransactionUniquenessMark,
    TransactionViewedRowMark,
)


type TransactionEffect = Effect[Any, Row, Never, Row, Row]


@dataclass(frozen=True)
class TransactionConflict:
    uniqueness_marks: frozenset[TransactionUniquenessMark]


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
    _is_commited: bool
    _effect: TransactionEffect
    _viewed_row_marks: set[TransactionViewedRowMark]
    _uniqueness_marks: set[TransactionUniquenessMark]
    _right: list["Transaction"]
    _left: list["Transaction"]
    _subset: list["Transaction"]
    _superset: list["Transaction"]

    def is_readonly(self) -> bool:
        return not any(self._effect) and not self._uniqueness_marks

    def consider(self, effect: RowEffect) -> None:
        """
        :raises effect.state_transition.InvalidStateTransitionError:
        """

        self._effect &= effect

    def add_uniqueness_mark(
        self, uniqueness_mark: TransactionUniquenessMark
    ) -> None:
        self._uniqueness_marks.add(uniqueness_mark)

    def add_viewed_row_mark(
        self, viewed_row_mark: TransactionViewedRowMark
    ) -> None:
        self._viewed_row_marks.add(viewed_row_mark)

    def beginning(self) -> LogicTime | None:
        return self._beginning

    def rollback(self) -> None:
        self._unlink_all()

    def commit(self, current_time: LogicTime) -> TransactionCommit:
        conflict = self._conflict()

        if conflict is not None:
            self.rollback()
            return TransactionFailedCommit(self.id, conflict)

        self._is_commited = True

        self._link_superset(
            left_transaction for left_transaction in self._left
            if not left_transaction._is_commited
        )
        self._unlink_left()
        self._unlink_subset()

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
        transaction = Transaction(
            id=transaction_id,
            _beginning=current_time,
            _is_commited=False,
            _effect=Effect(None),
            _viewed_row_marks=set(),
            _uniqueness_marks=set(),
            _right=list(),
            _left=list(),
            _subset=list(),
            _superset=list(),
        )
        transaction._link_left(active_transactions)

        return transaction

    def _conflict(self) -> TransactionConflict | None:
        for transaction in self._transactions_with_possible_conflict():
            conflict_uniqueness_marks = frozenset(
                self._uniqueness_marks & transaction._uniqueness_marks
            )

            conflict_rows = (
                IdentifiedValueSet(self._effect)
                & IdentifiedValueSet(transaction._effect)
            )

            if conflict_rows or conflict_uniqueness_marks:
                return TransactionConflict(
                    uniqueness_marks=conflict_uniqueness_marks
                )

        return None

    def _transactions_with_possible_conflict(
        self
    ) -> Iterable["Transaction"]:
        for transaction in chain(self._left, self._subset):
            if transaction._is_commited and not self.is_readonly():
                yield transaction

    def _link_left(self, transactions: Iterable["Transaction"]) -> None:
        for transaction in transactions:
            transaction._right.append(self)
            self._left.append(transaction)

    def _link_right(self, transactions: Iterable["Transaction"]) -> None:
        for transaction in transactions:
            transaction._left.append(self)
            self._right.append(transaction)

    def _link_subset(self, transactions: Iterable["Transaction"]) -> None:
        for transaction in transactions:
            transaction._superset.append(self)
            self._subset.append(transaction)

    def _link_superset(self, transactions: Iterable["Transaction"]) -> None:
        for transaction in transactions:
            transaction._subset.append(self)
            self._superset.append(transaction)

    def _unlink_left(self) -> None:
        for transaction in self._left:
            transaction._right.remove(self)

        self._left = list()

    def _unlink_right(self) -> None:
        for transaction in self._right:
            transaction._left.remove(self)

        self._right = list()

    def _unlink_subset(self) -> None:
        for transaction in self._subset:
            transaction._superset.remove(self)

        self._subset = list()

    def _unlink_superset(self) -> None:
        for transaction in self._superset:
            transaction._subset.remove(self)

        self._superset = list()

    def _unlink_all(self) -> None:
        self._unlink_left()
        self._unlink_right()
        self._unlink_subset()
        self._unlink_superset()
