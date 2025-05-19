from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import (
    AppliedOperator,
    DeletedRow,
    Mark,
    MutatedRow,
    NewRow,
    TransactionState,
)
from tgdb.entities.transaction import (
    Transaction,
    TransactionCommit,
    TransactionFailedCommit,
)


class NotStartedTransactionError(Exception): ...


class NonLinearizedOperatorError(Exception): ...


@dataclass
class TransactionHorizon:
    _max_len: int
    _time: LogicTime | None
    _active_transaction_by_id: OrderedDict[UUID, Transaction]

    def __bool__(self) -> bool:
        return self.beginning() is not None

    def __iter__(self) -> Iterator[Transaction]:
        return iter(self._active_transaction_by_id.values())

    def __len__(self) -> int:
        return len(self._active_transaction_by_id)

    def time(self) -> LogicTime | None:
        return self._time

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

        match operator.effect, transaction:
            case NewRow() | MutatedRow() | DeletedRow() | Mark(), Transaction():
                transaction.add_effect(operator.effect)

            case TransactionState.started, None:
                self._start_transaction(operator.transaction_id)

            case TransactionState.rollbacked, Transaction():
                del self._active_transaction_by_id[transaction.id]
                transaction.rollback()

            case TransactionState.committed, Transaction():
                del self._active_transaction_by_id[transaction.id]
                return transaction.commit(operator.time)

            case TransactionState.committed, None:
                return TransactionFailedCommit(
                    operator.transaction_id, conflict=None
                )

            case _:
                ...

        return None

    def _start_transaction(self, transaction_id: UUID) -> None:
        assert self._time is not None

        new_transaction = Transaction.start(
            transaction_id,
            self._active_transaction_by_id.values(),
            self._time,
        )
        self._active_transaction_by_id[transaction_id] = new_transaction

        if len(self) <= self._max_len:
            return

        oldest_transaction = self._oldest_transaction()
        assert oldest_transaction is not None

        oldest_transaction.rollback()
        del self._active_transaction_by_id[oldest_transaction.id]

    def _oldest_transaction(self) -> Transaction | None:
        try:
            return next(iter(self._active_transaction_by_id.values()))
        except StopIteration:
            return None


def create_transaction_horizon(max_len: int) -> TransactionHorizon:
    return TransactionHorizon(
        _time=None,
        _max_len=max_len,
        _active_transaction_by_id=OrderedDict(),
    )
