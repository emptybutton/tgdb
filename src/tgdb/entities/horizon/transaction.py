from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum, auto
from uuid import UUID

from tgdb.entities.horizon.effect import (
    Claim,
    ConflictableTransactionScalarEffect,
    TransactionEffect,
    TupleEffect,
    ViewedTuple,
)
from tgdb.entities.time.logic_time import LogicTime
from tgdb.entities.topic.partition import PartitionTupleID


@dataclass(frozen=True)
class TransactionConflict:
    transaction_id: UUID
    rejected_claims: frozenset[Claim]


@dataclass(frozen=True)
class NoTransaction:
    transaction_id: UUID


@dataclass(frozen=True)
class NonSerializableWriteTransaction:
    id: UUID


@dataclass(frozen=True)
class TransactionOkPreparedCommit:
    transaction_id: UUID
    effect: TransactionEffect


type TransactionFailedPreparedCommit = (
    TransactionConflict | NoTransaction | NonSerializableWriteTransaction
)

type TransactionPreparedCommit = (
    TransactionOkPreparedCommit | TransactionFailedPreparedCommit
)


@dataclass(frozen=True)
class TransactionCommit:
    transaction_id: UUID
    effect: TransactionEffect


class TransactionState(Enum):
    active = auto()
    rollbacked = auto()
    prepared = auto()
    commited = auto()


class Transaction(ABC):
    @abstractmethod
    def id(self) -> UUID: ...

    @abstractmethod
    def state(self) -> TransactionState: ...

    @abstractmethod
    def beginning(self) -> LogicTime: ...

    @abstractmethod
    def include(self, effect: ConflictableTransactionScalarEffect, /) -> None:
        ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def prepare_commit(self) -> TransactionPreparedCommit: ...

    @abstractmethod
    def commit(self) -> TransactionCommit: ...


@dataclass(eq=False, unsafe_hash=False)
class SerializableTransaction(Transaction):
    _id: UUID
    _state: TransactionState
    _beginning: LogicTime
    _space_map: dict[PartitionTupleID, TupleEffect]
    _claims: set[Claim]
    _concurrent_transactions: set["Transaction"]
    _transactions_with_possible_conflict: set["SerializableTransaction"]

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and self._id == other._id

    def __hash__(self) -> int:
        return hash(type(self)) + hash(self._id)

    def id(self) -> UUID:
        return self._id

    def state(self) -> TransactionState:
        return self._state

    def beginning(self) -> LogicTime:
        return self._beginning

    def include(self, effect: ConflictableTransactionScalarEffect) -> None:
        if isinstance(effect, Claim):
            self._claims.add(effect)
            return

        prevous_effect = self._space_map.get(effect.id)

        if prevous_effect is not None:
            effect = prevous_effect & effect

        self._space_map[effect.id] = effect

    def rollback(self) -> None:
        if self._state is TransactionState.prepared:
            for transaction in self._concurrent_transactions:
                if not isinstance(transaction, SerializableTransaction):
                    continue

                transaction._transactions_with_possible_conflict.remove(self)

        for transaction in self._concurrent_transactions:
            if not isinstance(transaction, SerializableTransaction):
                continue

            transaction._concurrent_transactions.remove(self)

        self._complete()
        self._state = TransactionState.rollbacked

    def prepare_commit(self) -> TransactionPreparedCommit:
        conflict = self._conflict()

        if conflict is not None:
            self.rollback()
            return conflict

        self._state = TransactionState.prepared

        for transaction in self._concurrent_transactions:
            if isinstance(transaction, SerializableTransaction):
                transaction._transactions_with_possible_conflict.add(self)

        return TransactionOkPreparedCommit(self._id, self._effect())

    def commit(self) -> TransactionCommit:
        self._complete()
        self._state = TransactionState.commited

        return TransactionCommit(self._id, self._effect())

    @classmethod
    def start(
        cls,
        transaction_id: UUID,
        active_transactions: Iterable["Transaction"],
        current_time: LogicTime,
    ) -> "SerializableTransaction":
        transasction = SerializableTransaction(
            _id=transaction_id,
            _state=TransactionState.active,
            _beginning=current_time,
            _space_map=dict(),
            _claims=set(),
            _concurrent_transactions=set(active_transactions),
            _transactions_with_possible_conflict=set(),
        )

        for active_transaction in active_transactions:
            if not isinstance(active_transaction, SerializableTransaction):
                continue

            if active_transaction._state is TransactionState.active:
                active_transaction._concurrent_transactions.add(transasction)

            if active_transaction._state is TransactionState.prepared:
                transasction._transactions_with_possible_conflict.add(
                    active_transaction
                )

        return transasction

    def _complete(self) -> None:
        self._concurrent_transactions.clear()
        self._transactions_with_possible_conflict.clear()

    def _conflict(self) -> TransactionConflict | None:
        for transaction in self._transactions_with_possible_conflict:
            conflict_claims = frozenset(self._claims & transaction._claims)
            conflict_space = self._space() & transaction._space()

            if conflict_claims or conflict_space:
                return TransactionConflict(
                    self._id, rejected_claims=conflict_claims
                )

        return None

    def _space(self) -> set[PartitionTupleID]:
        return set(self._space_map)

    def _effect(self) -> TransactionEffect:
        return set(
            scalar_effect
            for scalar_effect in self._space_map.values()
            if not isinstance(scalar_effect, ViewedTuple)
        )


@dataclass
class NonSerializableReadTransaction(Transaction):
    _id: UUID
    _state: TransactionState
    _beginning: LogicTime
    _is_readonly: bool

    def id(self) -> UUID:
        return self._id

    def state(self) -> TransactionState:
        return self._state

    def beginning(self) -> LogicTime:
        return self._beginning

    def include(self, effect: ConflictableTransactionScalarEffect) -> None:
        if self._is_readonly and not isinstance(effect, ViewedTuple):
            self._is_readonly = False

    def rollback(self) -> None:
        self._state = TransactionState.rollbacked

    def prepare_commit(self) -> TransactionPreparedCommit:
        self._state = TransactionState.prepared

        if not self._is_readonly:
            return NonSerializableWriteTransaction(self._id)

        return TransactionOkPreparedCommit(self._id, frozenset())

    def commit(self) -> TransactionCommit:
        self._state = TransactionState.commited
        return TransactionCommit(self._id, frozenset())

    @classmethod
    def start(
        cls,
        transaction_id: UUID,
        current_time: LogicTime,
    ) -> "NonSerializableReadTransaction":
        return NonSerializableReadTransaction(
            _id=transaction_id,
            _state=TransactionState.active,
            _beginning=current_time,
            _is_readonly=True,
        )


class TransactionIsolation(Enum):
    serializable_read_and_write = auto()
    non_serializable_read = auto()


def start_transaction(
    transaction_id: UUID,
    transaction_isolation: TransactionIsolation,
    active_transactions: Iterable[Transaction],
    time: LogicTime,
) -> Transaction:
    match transaction_isolation:
        case TransactionIsolation.serializable_read_and_write:
            return SerializableTransaction.start(
                transaction_id, active_transactions, time,
            )

        case TransactionIsolation.non_serializable_read:
            return NonSerializableReadTransaction.start(transaction_id, time)
