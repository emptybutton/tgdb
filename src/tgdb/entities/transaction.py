from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum, auto
from uuid import UUID

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.mark import Mark
from tgdb.entities.row import RowID, RowVersion


@dataclass(frozen=True)
class NewRow:
    row_version: RowVersion

    @property
    def id(self) -> RowID:
        return self.row_version.row_id()


@dataclass(frozen=True)
class ViewedRow:
    id: RowID


@dataclass(frozen=True)
class MutatedRow:
    row_version: RowVersion

    @property
    def id(self) -> RowID:
        return self.row_version.row_id()


@dataclass(frozen=True)
class DeletedRow:
    id: RowID


type TransactionScalarEffect = (
    NewRow | MutatedRow | DeletedRow | ViewedRow | Mark
)
type TransactionEffect = Sequence[TransactionScalarEffect]

type TransactionCommitScalarEffect = NewRow | MutatedRow | DeletedRow
type TransactionCommitEffect = Sequence[TransactionCommitScalarEffect]


@dataclass(frozen=True)
class TransactionConflict:
    transaction_id: UUID
    marks: frozenset[Mark]


@dataclass(frozen=True)
class NoTransaction:
    transaction_id: UUID


@dataclass(frozen=True)
class NonSerializableWriteTransaction:
    id: UUID


@dataclass(frozen=True)
class TransactionOkPreparedCommit:
    transaction_id: UUID
    effect: TransactionCommitEffect


type TransactionFailedPreparedCommit = (
    TransactionConflict | NoTransaction | NonSerializableWriteTransaction
)

type TransactionPreparedCommit = (
    TransactionOkPreparedCommit | TransactionFailedPreparedCommit
)


@dataclass(frozen=True)
class TransactionCommit:
    transaction_id: UUID
    effect: TransactionCommitEffect


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
    def add_effect(self, effect: TransactionScalarEffect, /) -> None: ...

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
    _commit_effect: list[TransactionCommitScalarEffect]
    _is_commit_effect_serializable: bool
    _commit_effect_row_ids: set[RowID]
    _not_commit_effect_row_ids: set[RowID]
    _marks: set[Mark]
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

    def add_effect(self, effect: TransactionScalarEffect) -> None:
        match effect:
            case NewRow() | MutatedRow() | DeletedRow() as row:
                if row.id in self._commit_effect_row_ids:
                    self._is_commit_effect_serializable = False
            case _: ...

        match effect:
            case NewRow() | MutatedRow() | DeletedRow() as row:
                self._commit_effect.append(row)
                self._commit_effect_row_ids.add(row.id)

            case ViewedRow() as row:
                self._not_commit_effect_row_ids.add(row.id)

            case Mark() as mark:
                self._marks.add(mark)

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
        if not self._is_commit_effect_serializable:
            self.rollback()
            return NonSerializableWriteTransaction(self._id)

        conflict = self._conflict()

        if conflict is not None:
            self.rollback()
            return conflict

        self._state = TransactionState.prepared

        for transaction in self._concurrent_transactions:
            if isinstance(transaction, SerializableTransaction):
                transaction._transactions_with_possible_conflict.add(self)

        return TransactionOkPreparedCommit(self._id, self._commit_effect)

    def commit(self) -> TransactionCommit:
        self._complete()
        self._state = TransactionState.commited

        return TransactionCommit(self._id, self._commit_effect)

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
            _is_commit_effect_serializable=True,
            _commit_effect=list(),
            _commit_effect_row_ids=set(),
            _not_commit_effect_row_ids=set(),
            _marks=set(),
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
            conflict_marks = frozenset(self._marks & transaction._marks)
            conflict_row_ids = self._row_ids() & transaction._row_ids()

            if conflict_marks or conflict_row_ids:
                return TransactionConflict(self._id, marks=conflict_marks)

        return None

    def _row_ids(self) -> set[RowID]:
        return self._commit_effect_row_ids | self._not_commit_effect_row_ids


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

    def add_effect(self, effect: TransactionScalarEffect) -> None:
        if self._is_readonly and not isinstance(effect, ViewedRow):
            self._is_readonly = False

    def rollback(self) -> None:
        self._state = TransactionState.rollbacked

    def prepare_commit(self) -> TransactionPreparedCommit:
        self._state = TransactionState.prepared

        if not self._is_readonly:
            return NonSerializableWriteTransaction(self._id)

        return TransactionOkPreparedCommit(self._id, tuple())

    def commit(self) -> TransactionCommit:
        self._state = TransactionState.commited
        return TransactionCommit(self._id, tuple())

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
