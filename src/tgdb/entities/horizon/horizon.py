from collections import OrderedDict
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass

from tgdb.entities.horizon.claim import Claim
from tgdb.entities.horizon.transaction import (
    XID,
    Commit,
    ConflictError,
    IsolationLevel,
    PreparedCommit,
    ReadUncommitedTransaction,
    SerializableTransaction,
    SerializableTransactionState,
    Transaction,
    start_transaction,
)
from tgdb.entities.relation.tuple_effect import (
    DeletedTuple,
    MutatedTuple,
    NewTuple,
    ViewedTuple,
)
from tgdb.entities.time.logic_time import LogicTime
from tgdb.entities.tools.assert_ import assert_
from tgdb.entities.tools.map import first_map_value


class NoTransactionError(Exception): ...


class DoubleStartTransactionError(Exception): ...


class TransactionCommittingError(Exception): ...


class TransactionNotCommittingError(Exception): ...


class HorizonAlwaysWithoutTransactionsError(Exception): ...


class NotMonotonicTimeError(Exception): ...


type HorizonWriteEffect = NewTuple | MutatedTuple | DeletedTuple | Claim


@dataclass
class Horizon:
    """
    :raises tgdb.entities.horizon.horizon.HorizonAlwaysWithoutTransactionsError:
    """

    _time: LogicTime
    _max_len: int
    _max_transaction_age: LogicTime
    _serializable_transaction_map: OrderedDict[XID, SerializableTransaction]
    _read_uncommited_transaction_map: OrderedDict[
        XID,
        ReadUncommitedTransaction,
    ]

    def __post_init__(self) -> None:
        assert_(
            self._max_len > 0 and self._max_transaction_age > 0,
            else_=HorizonAlwaysWithoutTransactionsError,
        )

    def __bool__(self) -> bool:
        return any(self._transaction_maps())

    def __len__(self) -> int:
        return sum(map(len, self._transaction_maps()))

    def start_transaction(
        self,
        time: LogicTime,
        xid: XID,
        isolation: IsolationLevel,
    ) -> XID:
        """
        :raises tgdb.entities.horizon.horizon.NotMonotonicTimeError:
        :raises tgdb.entities.horizon.horizon.DoubleStartTransactionError:
        """

        assert_(
            all(xid not in map_ for map_ in self._transaction_maps()),
            else_=DoubleStartTransactionError,
        )

        started_transaction = start_transaction(
            xid,
            self._time,
            isolation,
            self._serializable_transaction_map.values(),
        )

        map_ = self._transaction_map(started_transaction)
        map_[started_transaction.xid()] = started_transaction
        self._limit_len()
        self.move_to_future(time)

        return started_transaction.xid()

    def include(
        self,
        time: LogicTime,
        xid: XID,
        effect: ViewedTuple,
    ) -> None:
        """
        :raises tgdb.entities.horizon.horizon.NotMonotonicTimeError:
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.TransactionCommittingError:
        """

        self.move_to_future(time)

        transaction = self._transaction(
            xid,
            SerializableTransactionState.active,
            else_=TransactionCommittingError,
        )
        transaction.include(effect)

    def rollback_transaction(self, time: LogicTime, xid: XID) -> None:
        """
        :raises tgdb.entities.horizon.horizon.NotMonotonicTimeError:
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.TransactionCommittingError:
        """
        self.move_to_future(time)

        transaction = self._transaction(
            xid,
            SerializableTransactionState.active,
            else_=TransactionCommittingError,
        )
        transaction.rollback()
        del self._transaction_map(transaction)[transaction.xid()]

    def commit_transaction(
        self,
        time: LogicTime,
        xid: XID,
        effects: Sequence[HorizonWriteEffect],
    ) -> Commit | PreparedCommit:
        """
        :raises tgdb.entities.horizon.horizon.NotMonotonicTimeError:
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.TransactionCommittingError:
        :raises tgdb.entities.horizon.transaction.ConflictError:
        """
        self.move_to_future(time)

        transaction = self._transaction(
            xid,
            SerializableTransactionState.active,
            else_=TransactionCommittingError(),
        )

        for effect in effects:
            transaction.include(effect)

        try:
            match transaction:
                case SerializableTransaction():
                    return transaction.prepare_commit()
                case ReadUncommitedTransaction():
                    del self._transaction_map(transaction)[xid]
                    return transaction.commit()
        except ConflictError as error:
            del self._transaction_map(transaction)[xid]
            raise error from error

    def complete_commit(self, time: LogicTime, xid: XID) -> Commit:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.TransactionNotCommittingError:
        """

        self.move_to_future(time)

        transaction = self._serializable_transaction(
            xid,
            SerializableTransactionState.prepared,
            else_=TransactionNotCommittingError,
        )

        commit = transaction.commit()
        del self._serializable_transaction_map[transaction.xid()]

        return commit

    def move_to_future(self, time: LogicTime) -> None:
        """
        :raises tgdb.entities.horizon.horizon.NotMonotonicTimeError:
        """

        assert_(self._time < time, else_=NotMonotonicTimeError)
        self._time = time

        self._limit_transaction_age()

    def _limit_transaction_age(self) -> None:
        while True:
            oldest_transaction = self._oldest_transaction()

            if oldest_transaction is None:
                break

            if oldest_transaction.age(self._time) <= self._max_transaction_age:
                break

            if not self._is_transaction_autorollbackable(oldest_transaction):
                continue

            oldest_transaction.rollback()
            del self._transaction_map(oldest_transaction)[
                oldest_transaction.xid()
            ]

    def _limit_len(self) -> None:
        while len(self) > self._max_len:
            oldest_transaction = self._oldest_transaction()

            if oldest_transaction is None:
                return

            if not self._is_transaction_autorollbackable(oldest_transaction):
                continue

            oldest_transaction.rollback()
            del self._transaction_map(oldest_transaction)[
                oldest_transaction.xid()
            ]

    def _is_transaction_autorollbackable(
        self,
        transaction: Transaction,
    ) -> bool:
        return (
            not isinstance(transaction, SerializableTransaction)
            or transaction.state() is not SerializableTransactionState.prepared
        )

    def _oldest_transaction(self) -> Transaction | None:
        first_map_tranactions = (
            first_map_value(map_) for map_ in self._transaction_maps()
        )
        oldest_transactions = tuple(
            first_map_tranaction
            for first_map_tranaction in first_map_tranactions
            if first_map_tranaction is not None
        )

        if not oldest_transactions:
            return None

        return min(oldest_transactions, key=lambda it: it.start_time())

    def _serializable_transaction(
        self,
        xid: XID,
        state: SerializableTransactionState | None = None,
        *,
        else_: Exception | type[Exception],
    ) -> SerializableTransaction:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises else_:
        """

        transaction = self._serializable_transaction_map.get(xid)

        if transaction is None:
            raise NoTransactionError

        if state is not None and transaction.state() is not state:
            transaction.rollback()
            del self._serializable_transaction_map[xid]

            raise else_

        return transaction

    def _non_serializable_read_transaction(
        self,
        xid: XID,
    ) -> ReadUncommitedTransaction:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        """

        transaction = self._read_uncommited_transaction_map.get(xid)

        if transaction is None:
            raise NoTransactionError

        return transaction

    def _transaction(
        self,
        xid: XID,
        state: SerializableTransactionState | None = None,
        *,
        else_: Exception | type[Exception],
    ) -> Transaction:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises else_:
        """
        with suppress(NoTransactionError):
            return self._non_serializable_read_transaction(xid)

        return self._serializable_transaction(xid, state, else_=else_)

    def _transaction_maps(self) -> Iterable[Mapping[XID, Transaction]]:
        yield self._serializable_transaction_map
        yield self._read_uncommited_transaction_map

    def _transaction_map[TransactionT: Transaction](
        self,
        transaction: TransactionT,
    ) -> OrderedDict[XID, TransactionT]:
        match transaction:
            case ReadUncommitedTransaction():
                return self._read_uncommited_transaction_map  # type: ignore[return-value]
            case SerializableTransaction():
                return self._serializable_transaction_map  # type: ignore[return-value]


def horizon(
    time: LogicTime,
    max_len: int,
    max_transaction_age: LogicTime,
) -> Horizon:
    """
    :raises tgdb.entities.horizon.horizon.HorizonAlwaysWithoutTransactionsError:
    """
    return Horizon(
        _time=time,
        _max_len=max_len,
        _max_transaction_age=max_transaction_age,
        _serializable_transaction_map=OrderedDict(),
        _read_uncommited_transaction_map=OrderedDict(),
    )
