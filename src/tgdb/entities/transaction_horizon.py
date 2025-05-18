from collections import OrderedDict
from dataclasses import dataclass
from uuid import UUID

from effect import Effect

from tgdb.entities.logic_time import LogicTime, age
from tgdb.entities.operator import AppliedOperator
from tgdb.entities.transaction import (
    Transaction,
    TransactionCommit,
    TransactionConflict,
    TransactionFailedCommit,
)
from tgdb.entities.transaction_mark import (
    TransactionState,
    TransactionStateMark,
    TransactionUniquenessMark,
    TransactionViewedRowMark,
)


class NotStartedTransactionError(Exception): ...


class NonLinearizedOperatorError(Exception): ...


@dataclass
class TransactionHorizon:
    _max_age: LogicTime
    _time: LogicTime | None
    _active_transaction_by_id: OrderedDict[UUID, Transaction]

    def __bool__(self) -> bool:
        return self.beginning() is not None

    def beginning(self) -> LogicTime | None:
        oldest_transaction = self._oldest_transaction()

        if oldest_transaction is None:
            return None

        return oldest_transaction.beginning()

    def add(self, operator: AppliedOperator) -> TransactionCommit | None:
        if self._time is not None and operator.time <= self._time:
            raise NonLinearizedOperatorError

        self._time = operator.time

        transaction = self._active_transaction_by_id.get(
            operator.transaction_id
        )

        commit: TransactionCommit | None = None

        match operator, transaction:
            case AppliedOperator(Effect() as row_effect), Transaction():
                transaction.consider(row_effect)

            case (
                AppliedOperator(TransactionUniquenessMark() as mark),
                Transaction()
            ):
                transaction.add_uniqueness_mark(mark)

            case (
                AppliedOperator(TransactionViewedRowMark() as mark),
                Transaction()
            ):
                transaction.add_viewed_row_mark(mark)

            case (
                AppliedOperator(TransactionStateMark(TransactionState.started)),
                None
            ):
                new_transaction = Transaction.start(
                    operator.transaction_id,
                    self._active_transaction_by_id.values(),
                    operator.time,
                )
                self._active_transaction_by_id[operator.transaction_id] = (
                    new_transaction
                )

            case (
                AppliedOperator(TransactionStateMark(TransactionState.rollbacked)),
                Transaction(),
            ):
                transaction.rollback()
                del self._active_transaction_by_id[transaction.id]

            case (
                AppliedOperator(TransactionStateMark(TransactionState.committed)),
                Transaction(),
            ):
                commit = transaction.commit(operator.time)
                del self._active_transaction_by_id[transaction.id]

            case (
                AppliedOperator(TransactionStateMark(TransactionState.committed)),
                None,
            ):
                return TransactionFailedCommit(
                    operator.transaction_id, TransactionConflict(frozenset())
                )

            case _:
                ...

        self._limit_age(operator.time)
        return commit

    def _oldest_transaction(self) -> Transaction | None:
        try:
            return next(iter(self._active_transaction_by_id.values()))
        except StopIteration:
            return None

    def _limit_age(self, current_time: LogicTime) -> None:
        transactions_to_remove = list[Transaction]()

        for transaction in self._active_transaction_by_id.values():
            if age(current_time, transaction.beginning()) > self._max_age:
                transactions_to_remove.append(transaction)
            else:
                break

        for transaction in transactions_to_remove:
            del self._active_transaction_by_id[transaction.id]


def create_transaction_horizon(max_age: LogicTime) -> TransactionHorizon:
    return TransactionHorizon(
        _time=None,
        _max_age=max_age,
        _active_transaction_by_id=OrderedDict(),
    )
