from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from tgdb.entities.horizon.effect import (
    Claim,
    DeletedTuple,
    MutatedTuple,
    NewTuple,
    ViewedTuple,
)
from tgdb.entities.horizon.transaction import (
    Transaction,
    TransactionCommit,
    TransactionIsolation,
    TransactionOkPreparedCommit,
    TransactionPreparedCommit,
    TransactionState,
    start_transaction,
)
from tgdb.entities.tools.assert_ import not_none


class NoTransactionError(Exception): ...


class InvalidTransactionStateError(Exception): ...


@dataclass
class Horizon:
    _max_len: int
    _transaction_map: OrderedDict[UUID, Transaction]

    def __bool__(self) -> bool:
        return bool(self._transaction_map)

    def __len__(self) -> int:
        return len(self._transaction_map)

    def start_transaction(
        self,
        transaction_id: UUID,
        transaction_isolation: TransactionIsolation,
    ) -> Transaction:
        """
        :raises tgdb.entities.horizon.horizon.InvalidTransactionStateError:
        """

        if transaction_id in self._transaction_map:
            raise InvalidTransactionStateError

        new_transaction = start_transaction(
            transaction_id,
            transaction_isolation,
            self._transaction_map.values(),
        )
        self._transaction_map[new_transaction.id()] = new_transaction
        self._limit_len()

        return new_transaction

    def view_tuple(
        self,
        transaction_id: UUID,
        effect: ViewedTuple,
    ) -> None:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.InvalidTransactionStateError:
        """

        transaction = self._transaction(transaction_id, TransactionState.active)
        transaction.include(effect)

    def rollback_transaction(self, transaction_id: UUID) -> None:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        """

        transaction = self._transaction(transaction_id)
        transaction.rollback()
        del self._transaction_map[transaction.id()]

    def commit_transaction(
        self,
        transaction_id: UUID,
        effects: Sequence[NewTuple | MutatedTuple | DeletedTuple | Claim],
    ) -> TransactionPreparedCommit:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.InvalidTransactionStateError:
        """

        transaction = self._transaction(transaction_id, TransactionState.active)

        for effect in effects:
            transaction.include(effect)

        return transaction.prepare_commit()

    def complete_commit(
        self, prepared_commit: TransactionOkPreparedCommit
    ) -> TransactionCommit:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.InvalidTransactionStateError:
        """

        transaction = self._transaction(
            prepared_commit.transaction_id,
            TransactionState.prepared,
        )

        commit = transaction.commit()
        del self._transaction_map[transaction.id()]

        return commit

    def _limit_len(self) -> None:
        if len(self) > self._max_len:
            transaction = not_none(self._oldest_transaction())
            transaction.rollback()
            del self._transaction_map[transaction.id()]

    def _oldest_transaction(self) -> Transaction | None:
        try:
            return next(iter(self._transaction_map.values()))
        except StopIteration:
            return None

    def _transaction(
        self, id: UUID, state: TransactionState | None = None
    ) -> Transaction:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.InvalidTransactionStateError:
        """

        transaction = self._transaction_map.get(id)

        if transaction is None:
            raise NoTransactionError

        if state is not None and transaction.state() is not state:
            raise InvalidTransactionStateError

        return transaction


def horizon(max_len: int) -> Horizon:
    return Horizon(_max_len=max_len, _transaction_map=OrderedDict())
