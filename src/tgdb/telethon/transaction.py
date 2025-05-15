from asyncio.locks import Event
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from functools import partial
from itertools import chain
from typing import cast
from uuid import UUID

from effect import LifeCycle, many
from pydantic import BaseModel

from tgdb.telethon.row import Row, RowAttribute
from tgdb.telethon.transaction_operator.row_in_transaction import effect_from_row_in_transaction


# class TransactionConflict(BaseModel):
#     uniqueness_marks: frozenset[str]


# class TransactionResult(BaseModel):
#     transaction_id: UUID
#     conflict: TransactionConflict | None

#     def is_transaction_applied(self) -> bool:
#         return self.conflict is None


class TransactionConflict(BaseModel):
    uniqueness_marks: frozenset[str]


@dataclass(frozen=True)
class TransactionOkCommit:
    transaction_id: UUID
    effect: LifeCycle[Row]

    def is_transaction_applied(self) -> bool:
        return True


@dataclass(frozen=True)
class TransactionFailedCommit:
    transaction_id: UUID
    conflict: TransactionConflict

    def is_transaction_applied(self) -> bool:
        return False


type TransactionCommit = TransactionOkCommit | TransactionFailedCommit


class NotStartedTransactionError(Exception): ...


@dataclass
class Transaction:
    id: UUID
    _start_message_id: int | None = field(default=False, init=False)
    _is_commited: bool = field(default=False, init=False)
    _rows_in_transaction: list[Row] = field(default_factory=list, init=False)
    _viewed_row_marks: list[Row] = field(default_factory=list, init=False)
    _uniqueness_marks: list[Row] = field(default_factory=list, init=False)
    _right: list["Transaction"] = field(default_factory=list, init=False)
    _left: list["Transaction"] = field(default_factory=list, init=False)
    _subset: list["Transaction"] = field(default_factory=list, init=False)
    _superset: list["Transaction"] = field(default_factory=list, init=False)

    def is_readonly(self) -> bool:
        return not self._rows_in_transaction and not self._uniqueness_marks

    def add_row(self, row: Row) -> None:
        self._rows_in_transaction.append(row)

    def add_uniqueness_mark(self, uniqueness_mark: Row) -> None:
        self._uniqueness_marks.append(uniqueness_mark)

    def start_message_id(self) -> int:
        if self._start_message_id is None:
            raise NotStartedTransactionError

        return self._start_message_id

    def start(
        self,
        active_transactions: Iterable["Transaction"],
        start_message_id: int,
    ) -> None:
        self._start_message_id = start_message_id
        self._link_left(active_transactions)

    def rollback(self) -> None:
        self._unlink_all()

    async def commit(self) -> TransactionCommit:
        conflict = self._conflict()

        if conflict is not None:
            self.rollback()
            return TransactionFailedCommit(self.id, conflict)

        self._is_commited = True

        self._link_superset(
            left_transaction for left_transaction in self._left
            if not left_transaction._is_commited  # noqa: SLF001
        )
        self._unlink_left()
        self._unlink_subset()

        effect = (
            many(map(effect_from_row_in_transaction, self._rows_in_transaction))
        )

        return TransactionOkCommit(transaction_id=self.id, conflict=None)

    def _conflict(self) -> TransactionConflict | None:
        for transaction in self._transactions_with_possible_conflict():
            conflict_uniqueness_mark_values = (
                self._uniqueness_mark_values()
                & transaction._uniqueness_mark_values()  # noqa: SLF001
            )

            conflict_row_ids = self._row_ids() & transaction._row_ids()

            if conflict_row_ids or conflict_uniqueness_mark_values:
                return TransactionConflict(
                    uniqueness_marks=conflict_uniqueness_mark_values
                )

        return None

    def _transactions_with_possible_conflict(
        self
    ) -> Iterable["Transaction"]:
        for transaction in chain(self._left, self._subset):
            if transaction._is_commited and not self.is_readonly():  # noqa: SLF001
                yield transaction

    def _row_ids(self) -> frozenset[RowAttribute]:
        return (
            frozenset(row.id for row in self._rows_in_transaction)
            | frozenset(mark.id for mark in self._viewed_row_marks)
        )

    def _uniqueness_mark_values(self) -> frozenset[str]:
        return frozenset(cast(str, mark[1]) for mark in self._uniqueness_marks)

    def _link_left(self, transactions: Iterable["Transaction"]) -> None:
        for transaction in transactions:
            transaction._right.append(self)  # noqa: SLF001
            self._left.append(transaction)

    def _link_right(self, transactions: Iterable["Transaction"]) -> None:
        for transaction in transactions:
            transaction._left.append(self)  # noqa: SLF001
            self._right.append(transaction)

    def _link_subset(self, transactions: Iterable["Transaction"]) -> None:
        for transaction in transactions:
            transaction._superset.append(self)  # noqa: SLF001
            self._subset.append(transaction)

    def _link_superset(self, transactions: Iterable["Transaction"]) -> None:
        for transaction in transactions:
            transaction._subset.append(self)  # noqa: SLF001
            self._superset.append(transaction)

    def _unlink_left(self) -> None:
        for transaction in self._left:
            transaction._right.remove(self)  # noqa: SLF001

        self._left = list()

    def _unlink_right(self) -> None:
        for transaction in self._right:
            transaction._left.remove(self)  # noqa: SLF001

        self._right = list()

    def _unlink_subset(self) -> None:
        for transaction in self._subset:
            transaction._superset.remove(self)  # noqa: SLF001

        self._subset = list()

    def _unlink_superset(self) -> None:
        for transaction in self._superset:
            transaction._subset.remove(self)  # noqa: SLF001

        self._superset = list()

    def _unlink_all(self) -> None:
        self._unlink_left()
        self._unlink_right()
        self._unlink_subset()
        self._unlink_superset()


@dataclass(frozen=True, unsafe_hash=False)
class AsyncTransactionResults:
    _event_by_transaction_id: defaultdict[UUID, Event] = field(
        init=False, default_factory=partial(defaultdict, Event)
    )
    _result_by_transaction_id: dict[UUID, TransactionResult] = field(
        init=False, default_factory=dict
    )

    def __setitem__(
        self, transaction_id: UUID, transaction_result: TransactionResult
    ) -> None:
        self._result_by_transaction_id[transaction_id] = transaction_result
        self._event_by_transaction_id[transaction_id].set()

    async def __getitem__(self, transaction_id: UUID) -> TransactionResult:
        await self._event_by_transaction_id[transaction_id].wait()

        result = self._result_by_transaction_id[transaction_id]
        self._remove_transaction_result(transaction_id)

        return result

    def _remove_transaction_result(self, transaction_id: UUID) -> None:
        del self._result_by_transaction_id[transaction_id]
        del self._event_by_transaction_id[transaction_id]
