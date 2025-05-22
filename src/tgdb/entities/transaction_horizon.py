from collections import OrderedDict
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.assert_ import not_none
from tgdb.entities.logic_time import LogicTime, age
from tgdb.entities.operator import (
    AppliedOperator,
    CommitOperator,
    Operator,
    RollbackOperator,
    StartOperator,
)
from tgdb.entities.transaction import (
    NoTransaction,
    Transaction,
    TransactionCommit,
    TransactionFailedCommit,
)


class NonLinearizedOperatorError(Exception): ...


class UnlimitedTransactionHorizonError(Exception): ...


class UnattainableTransactionHorizonError(Exception): ...


class UselessMaxHeightError(Exception): ...


@dataclass
class TransactionHorizon:
    _max_width: LogicTime | None
    _max_height: int | None
    _time: LogicTime | None
    _active_transaction_by_id: OrderedDict[UUID, Transaction]

    def __post_init__(self) -> None:
        """
        :raises tgdb.entities.transaction_horizon.UnlimitedTransactionHorizonError:
        :raises tgdb.entities.transaction_horizon.UnattainableTransactionHorizonError:
        :raises tgdb.entities.transaction_horizon.UselessMaxHeightError:
        """  # noqa: E501

        if self._max_width is None and self._max_height is None:
            raise UnlimitedTransactionHorizonError

        if self._max_width is not None and self._max_width <= 0:
            raise UnattainableTransactionHorizonError

        if self._max_height is not None and self._max_height <= 0:
            raise UnattainableTransactionHorizonError

        if (
            self._max_height is not None and self._max_width is not None
            and self._max_width < self._max_height
        ):
            raise UselessMaxHeightError

    def __bool__(self) -> bool:
        return bool(self._active_transaction_by_id)

    def height(self) -> int:
        return len(self._active_transaction_by_id)

    def width(self) -> LogicTime:
        return age(self.beginning(), self.time())

    def time(self) -> LogicTime | None:
        return self._time

    def beginning(self) -> LogicTime | None:
        oldest_transaction = self._oldest_transaction()

        if oldest_transaction is None:
            return None

        return oldest_transaction.beginning()

    def add(
        self, applied_operator: AppliedOperator
    ) -> TransactionCommit | None:
        """
        :raises tgdb.entities.transaction_horizon.NonLinearizedOperatorError:
        """

        if self._time is not None and applied_operator.time <= self._time:
            raise NonLinearizedOperatorError

        self._time = applied_operator.time

        return self._add_operator(applied_operator.operator)

    def _add_operator(self, operator: Operator) -> TransactionCommit | None:
        transaction = self._active_transaction_by_id.get(
            operator.transaction_id
        )

        match operator, transaction:
            case StartOperator(), None:
                self._start_transaction(operator.transaction_id)

            case RollbackOperator(), Transaction():
                self._rollback(transaction)
                return None

            case CommitOperator(_, intermediate_operators), Transaction():
                for intermediate_operator in intermediate_operators:
                    transaction.add_operator(intermediate_operator)

                return self._commit(transaction, )

            case CommitOperator(), None:
                return TransactionFailedCommit(
                    operator.transaction_id, reason=NoTransaction()
                )

            case _:
                ...

        return None

    def _start_transaction(self, transaction_id: UUID) -> None:
        new_transaction = Transaction.start(
            transaction_id,
            self._active_transaction_by_id.values(),
            not_none(self._time),
        )
        self._active_transaction_by_id[transaction_id] = new_transaction

        too_wide = (
            self._max_width is not None and self.width() > self._max_width
        )
        too_high = (
            self._max_height is not None and self.height() > self._max_height
        )

        if too_wide or too_high:
            self._rollback(not_none(self._oldest_transaction()))

    def _rollback(self, transaction: Transaction) -> None:
        del self._active_transaction_by_id[transaction.id]
        transaction.rollback()

    def _commit(self, transaction: Transaction) -> TransactionCommit:
        del self._active_transaction_by_id[transaction.id]
        return transaction.commit()

    def _oldest_transaction(self) -> Transaction | None:
        try:
            return next(iter(self._active_transaction_by_id.values()))
        except StopIteration:
            return None


def create_transaction_horizon(
    max_width: LogicTime | None,
    max_height: int | None,
) -> TransactionHorizon:
    """
    :raises tgdb.entities.transaction_horizon.UnlimitedTransactionHorizonError:
    :raises tgdb.entities.transaction_horizon.UnattainableTransactionHorizonError:
    :raises tgdb.entities.transaction_horizon.UselessMaxHeightError:
    """  # noqa: E501

    return TransactionHorizon(
        _max_width=max_width,
        _max_height=max_height,
        _time=None,
        _active_transaction_by_id=OrderedDict(),
    )
