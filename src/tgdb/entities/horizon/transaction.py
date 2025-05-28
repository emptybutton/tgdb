from collections.abc import Iterable, Sequence, Set
from dataclasses import dataclass
from enum import Enum, auto
from uuid import UUID

from tgdb.entities.horizon.claim import Claim
from tgdb.entities.relation.tuple import TID
from tgdb.entities.relation.tuple_effect import (
    DeletedTuple,
    MigratedTuple,
    MutatedTuple,
    NewTuple,
    TupleOkEffect,
    ViewedTuple,
)
from tgdb.entities.time.logic_time import LogicTime


type XID = UUID

type ConflictableTransactionScalarEffect = TupleOkEffect | Claim
type ConflictableTransactionEffect = Sequence[
    ConflictableTransactionScalarEffect
]

type TransactionScalarEffect = (
    NewTuple | MutatedTuple | MigratedTuple | DeletedTuple
)
type TransactionEffect = Set[TransactionScalarEffect]


@dataclass(frozen=True)
class ConflictError(Exception):
    xid: XID
    rejected_claims: frozenset[Claim]


@dataclass(frozen=True)
class NonSerializableWriteTransactionError(Exception):
    xid: XID


@dataclass(frozen=True)
class Commit:
    xid: XID
    effect: TransactionEffect


@dataclass(frozen=True)
class PreparedCommit:
    xid: XID
    effect: TransactionEffect


class SerializableTransactionState(Enum):
    active = auto()
    rollbacked = auto()
    prepared = auto()
    commited = auto()


@dataclass(eq=False, unsafe_hash=False)
class SerializableTransaction:
    _xid: XID
    _start_time: LogicTime
    _state: SerializableTransactionState
    _space_map: dict[TID, TupleOkEffect]
    _claims: set[Claim]
    _concurrent_transactions: set["SerializableTransaction"]
    _transactions_with_possible_conflict: set["SerializableTransaction"]

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, SerializableTransaction)
            and self.xid() == other.xid()
        )

    def __hash__(self) -> int:
        return hash(type(self)) + hash(self._xid)

    def xid(self) -> XID:
        return self._xid

    def start_time(self) -> LogicTime:
        return self._start_time

    def age(self, time: LogicTime) -> LogicTime:
        return time - self._start_time

    def state(self) -> SerializableTransactionState:
        return self._state

    def claims(self) -> frozenset[Claim]:
        return frozenset(self._claims)

    def space(self) -> frozenset[TID]:
        return frozenset(self._space_map)

    def include(self, effect: ConflictableTransactionScalarEffect) -> None:
        if isinstance(effect, Claim):
            self._claims.add(effect)
            return

        prevous_effect = self._space_map.get(effect.tid)

        if prevous_effect is not None:
            effect = prevous_effect & effect

        self._space_map[effect.tid] = effect

    def rollback(self) -> None:
        state_before_rollback = self._state

        self._state = SerializableTransactionState.rollbacked

        if state_before_rollback is SerializableTransactionState.prepared:
            for transaction in self._concurrent_transactions:
                transaction.track_rollbacked_prepared_transaction(self)

        elif state_before_rollback is SerializableTransactionState.active:
            for transaction in self._concurrent_transactions:
                transaction.track_rollbacked_active_transaction(self)

        self._complete()

    def prepare_commit(self) -> PreparedCommit:
        """
        :raises tgdb.entities.horizon.transaction.ConflictError:
        """

        conflict = self._conflict()

        if conflict is not None:
            self.rollback()
            raise conflict

        commit = PreparedCommit(self._xid, self._effect())
        self._state = SerializableTransactionState.prepared

        for concurrent_transaction in self._concurrent_transactions:
            concurrent_transaction.track_prepared_transaction(self)

        self._complete()
        return commit

    def commit(self) -> Commit:
        self._state = SerializableTransactionState.commited

        return Commit(self._xid, self._effect())

    def track_concurrent_transaction(
        self, transaction: "SerializableTransaction"
    ) -> None:
        self._concurrent_transactions.add(transaction)

        if transaction.state() is SerializableTransactionState.prepared:
            self._transactions_with_possible_conflict.add(transaction)

    def track_started_transaction(
        self, started_transaction: "SerializableTransaction"
    ) -> None:
        self._concurrent_transactions.add(started_transaction)

    def track_prepared_transaction(
        self, prepared_transaction: "SerializableTransaction"
    ) -> None:
        self._transactions_with_possible_conflict.add(prepared_transaction)

    def track_rollbacked_prepared_transaction(
        self, rollbacked_prepared_transaction: "SerializableTransaction"
    ) -> None:
        if self._state is SerializableTransactionState.active:
            self._transactions_with_possible_conflict.remove(
                rollbacked_prepared_transaction
            )
            self._concurrent_transactions.remove(
                rollbacked_prepared_transaction
            )

    def track_rollbacked_active_transaction(
        self, rollbacked_active_transaction: "SerializableTransaction"
    ) -> None:
        if self._state is SerializableTransactionState.active:
            self._concurrent_transactions.remove(rollbacked_active_transaction)

    @classmethod
    def start(
        cls,
        xid: XID,
        time: LogicTime,
        concurrent_transactions: Iterable["SerializableTransaction"],
    ) -> "SerializableTransaction":
        started_transaction = SerializableTransaction(
            _xid=xid,
            _start_time=time,
            _state=SerializableTransactionState.active,
            _space_map=dict(),
            _claims=set(),
            _concurrent_transactions=set(),
            _transactions_with_possible_conflict=set(),
        )

        for concurrent_transaction in concurrent_transactions:
            concurrent_transaction.track_started_transaction(
                started_transaction
            )
            started_transaction.track_concurrent_transaction(
                concurrent_transaction
            )

        return started_transaction

    def _conflict(self) -> ConflictError | None:
        for transaction in self._transactions_with_possible_conflict:
            conflict_claims = self.claims() & transaction.claims()
            conflict_space = self.space() & transaction.space()

            if conflict_claims or conflict_space:
                return ConflictError(self._xid, rejected_claims=conflict_claims)

        return None

    def _effect(self) -> TransactionEffect:
        return set(
            scalar_effect
            for scalar_effect in self._space_map.values()
            if not isinstance(scalar_effect, ViewedTuple)
        )

    def _complete(self) -> None:
        self._concurrent_transactions.clear()
        self._transactions_with_possible_conflict.clear()


@dataclass
class NonSerializableReadTransaction:
    _xid: XID
    _start_time: LogicTime
    _is_readonly: bool
    _is_completed: bool

    def xid(self) -> XID:
        return self._xid

    def start_time(self) -> LogicTime:
        return self._start_time

    def age(self, time: LogicTime) -> LogicTime:
        return time - self._start_time

    def include(self, effect: ConflictableTransactionScalarEffect) -> None:
        if (
            self._is_readonly
            and not isinstance(effect, ViewedTuple | MigratedTuple)
        ):
            self._is_readonly = False

    def rollback(self) -> None:
        self._is_completed = True

    def commit(self) -> Commit:
        """
        :raises tgdb.entities.horizon.transaction.NonSerializableWriteTransactionError:
        """  # noqa: E501

        self._is_completed = True

        if not self._is_readonly:
            raise NonSerializableWriteTransactionError(self._xid)

        return Commit(self._xid, frozenset())

    @classmethod
    def start(
        cls, xid: XID, time: LogicTime
    ) -> "NonSerializableReadTransaction":
        return NonSerializableReadTransaction(
            _xid=xid,
            _start_time=time,
            _is_readonly=True,
            _is_completed=False,
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, SerializableTransaction)
            and self.xid() == other.xid()
        )

    def __hash__(self) -> int:
        return hash(type(self)) + hash(self._xid)


type Transaction = SerializableTransaction | NonSerializableReadTransaction


class IsolationLevel(Enum):
    serializable_read_and_write = auto()
    non_serializable_read = auto()


def start_transaction(
    xid: XID,
    time: LogicTime,
    isolation: IsolationLevel,
    serializable_transactions: Iterable[SerializableTransaction],
) -> Transaction:
    match isolation:
        case IsolationLevel.serializable_read_and_write:
            return SerializableTransaction.start(
                xid, time, serializable_transactions
            )

        case IsolationLevel.non_serializable_read:
            return NonSerializableReadTransaction.start(xid, time)
