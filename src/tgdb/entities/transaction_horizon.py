from dataclasses import dataclass
from uuid import UUID

from effect import Effect

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.entities.transaction import (
    Transaction,
    TransactionCommit,
)
from tgdb.entities.transaction_mark import (
    TransactionState,
    TransactionStateMark,
    TransactionUniquenessMark,
    TransactionViewedRowMark,
)


@dataclass
class TransactionHorizon:
    _beginning: LogicTime | None
    _active_transaction_by_id: dict[UUID, Transaction]

    def __bool__(self) -> bool:
        return self._beginning is not None

    def beginning(self) -> LogicTime | None:
        return self._beginning

    def take(self, operator: AppliedOperator) -> TransactionCommit | None:
        transaction = self._active_transaction_by_id.get(
            operator.transaction_id
        )

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

                if self._beginning is None:
                    self._beginning = operator.time

            case (
                AppliedOperator(TransactionStateMark(TransactionState.rollbacked)),
                Transaction(),
            ):
                del self._active_transaction_by_id[operator.transaction_id]

                transaction.rollback()
                self._refresh_beginning()

            case (
                AppliedOperator(TransactionStateMark(TransactionState.committed)),
                Transaction(),
            ):
                del self._active_transaction_by_id[operator.transaction_id]

                commit = transaction.commit()
                self._refresh_beginning()

                return commit

            case _:
                ...

        return None

    def _refresh_beginning(self) -> None:
        active_transaction_offsets = (
            transaction.beginning()
            for transaction in self._active_transaction_by_id.values()
        )
        self._beginning = min(active_transaction_offsets, default=None)


def create_transaction_horizon() -> TransactionHorizon:
    return TransactionHorizon(
        _beginning=None,
        _active_transaction_by_id=dict(),
    )
