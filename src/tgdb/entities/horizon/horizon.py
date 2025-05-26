from collections import OrderedDict
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.horizon.operator import (
    AppliedOperator,
    CommitOperator,
    IntermediateOperator,
    Operator,
    RollbackOperator,
    StartOperator,
)
from tgdb.entities.horizon.transaction import (
    NoTransaction,
    Transaction,
    TransactionCommit,
    TransactionOkPreparedCommit,
    TransactionPreparedCommit,
    TransactionState,
    start_transaction,
)
from tgdb.entities.time.logic_time import LogicTime, age
from tgdb.entities.tools.assert_ import not_none


class NonLinearizedOperatorError(Exception): ...


class UnlimitedHorizonError(Exception): ...


class UnattainableHorizonError(Exception): ...


class UselessMaxHeightError(Exception): ...


@dataclass
class Horizon:
    _max_width: LogicTime | None
    _max_height: int | None
    _time: LogicTime | None
    _active_transaction_by_id: OrderedDict[UUID, Transaction]

    def __post_init__(self) -> None:
        """
        :raises tgdb.entities.horizon.horizon.UnlimitedHorizonError:
        :raises tgdb.entities.horizon.horizon.UnattainableHorizonError:
        :raises tgdb.entities.horizon.horizon.UselessMaxHeightError:
        """

        if self._max_width is None and self._max_height is None:
            raise UnlimitedHorizonError

        if self._max_width is not None and self._max_width <= 0:
            raise UnattainableHorizonError

        if self._max_height is not None and self._max_height <= 0:
            raise UnattainableHorizonError

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

    def complete(
        self, prepared_commit: TransactionOkPreparedCommit
    ) -> TransactionCommit | None:
        transaction = self._active_transaction_by_id.get(
            prepared_commit.transaction_id
        )

        if (
            transaction is None
            or transaction.state() is not TransactionState.prepared
        ):
            return None

        commit = transaction.commit()
        del self._active_transaction_by_id[transaction.id()]

        return commit

    def add(
        self, applied_operator: AppliedOperator
    ) -> TransactionPreparedCommit | None:
        """
        :raises tgdb.entities.horizon.horizon.NonLinearizedOperatorError:
        """

        if self._time is not None and applied_operator.time <= self._time:
            raise NonLinearizedOperatorError

        self._time = applied_operator.time

        return self._add_operator(applied_operator.operator)

    def _add_operator(
        self, operator: Operator
    ) -> TransactionPreparedCommit | None:
        transaction = self._active_transaction_by_id.get(
            operator.transaction_id
        )
        state = None if transaction is None else transaction.state

        match operator, transaction, state:
            case StartOperator(), None, _:
                self._start_transaction(operator)

            case RollbackOperator(), Transaction(), TransactionState.active:
                self._rollback(transaction)
                return None

            case (
                IntermediateOperator(_, effect),
                Transaction(),
                TransactionState.active
            ):
                transaction.include(effect)

            case (
                CommitOperator(_, intermediate_operators),
                Transaction(),
                TransactionState.active
            ):
                for intermediate_operator in intermediate_operators:
                    transaction.include(intermediate_operator.effect)

                return transaction.prepare_commit()

            case CommitOperator(), None, _:
                return NoTransaction(operator.transaction_id)

            case _:
                ...

        return None

    def _start_transaction(self, operator: StartOperator) -> None:
        new_transaction = start_transaction(
            operator.transaction_id,
            operator.transaction_isolation,
            self._active_transaction_by_id.values(),
            not_none(self._time),
        )
        self._active_transaction_by_id[new_transaction.id()] = new_transaction

        self._limit_size()

    def _limit_size(self) -> None:
        too_wide = (
            self._max_width is not None and self.width() > self._max_width
        )
        too_high = (
            self._max_height is not None and self.height() > self._max_height
        )

        if too_wide or too_high:
            self._rollback(not_none(self._oldest_transaction()))

    def _rollback(self, transaction: Transaction) -> None:
        del self._active_transaction_by_id[transaction.id()]
        transaction.rollback()

    def _oldest_transaction(self) -> Transaction | None:
        try:
            return next(iter(self._active_transaction_by_id.values()))
        except StopIteration:
            return None


def create_horizon(
    max_width: LogicTime | None,
    max_height: int | None,
) -> Horizon:
    """
    :raises tgdb.entities.horizon.horizon.UnlimitedHorizonError:
    :raises tgdb.entities.horizon.horizon.UnattainableHorizonError:
    :raises tgdb.entities.horizon.horizon.UselessMaxHeightError:
    """

    return Horizon(
        _max_width=max_width,
        _max_height=max_height,
        _time=None,
        _active_transaction_by_id=OrderedDict(),
    )
