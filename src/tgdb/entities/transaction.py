from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum, auto
from uuid import UUID

from tgdb.entities.logic_time import LogicTime
from tgdb.entities.mark import Mark
from tgdb.entities.operator import IntermediateOperatorEffect
from tgdb.entities.row import (
    DeletedRow,
    MutatedRow,
    NewRow,
    RowID,
    ViewedRow,
)


type TransactionScalarEffect = NewRow | MutatedRow | DeletedRow
type TransactionEffect = Sequence[TransactionScalarEffect]


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
class TransactionOkCommit:
    transaction_id: UUID
    effect: TransactionEffect


type TransactionFailedCommit = (
    TransactionConflict | NoTransaction | NonSerializableWriteTransaction
)
type TransactionCommit = TransactionOkCommit | TransactionFailedCommit


class Transaction(ABC):
    @abstractmethod
    def id(self) -> UUID: ...

    @abstractmethod
    def beginning(self) -> LogicTime: ...

    @abstractmethod
    def add_effect(self, effect: IntermediateOperatorEffect, /) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def commit(self) -> TransactionCommit: ...


@dataclass
class SerializableTransaction(Transaction):
    _id: UUID
    _beginning: LogicTime
    _effect: list[TransactionScalarEffect]
    _is_serializable: bool
    _row_ids: set[RowID]
    _marks: set[Mark]
    _concurrent_transactions: list["Transaction"]
    _transactions_with_possible_conflict: list["SerializableTransaction"]

    def id(self) -> UUID:
        return self._id

    def beginning(self) -> LogicTime:
        return self._beginning

    def add_effect(self, effect: IntermediateOperatorEffect) -> None:
        match effect:
            case NewRow() | MutatedRow() | DeletedRow() | ViewedRow() as row:
                if row.id in self._row_ids:
                    self._is_serializable = False
            case _: ...

        match effect:
            case NewRow() | MutatedRow() | DeletedRow() as row:
                self._effect.append(row)
                self._row_ids.add(row.id)

            case ViewedRow() as row:
                self._row_ids.add(row.id)

            case Mark() as mark:
                self._marks.add(mark)

    def rollback(self) -> None:
        self._die()

    def commit(self) -> TransactionCommit:
        if not self._is_serializable:
            return NonSerializableWriteTransaction(self._id)

        conflict = self._conflict()

        if conflict is not None:
            self._die()
            return conflict

        for transaction in self._concurrent_transactions:
            if isinstance(transaction, SerializableTransaction):
                transaction._transactions_with_possible_conflict.append(self)

        self._die()
        return TransactionOkCommit(transaction_id=self._id, effect=self._effect)

    @classmethod
    def start(
        cls,
        transaction_id: UUID,
        active_transactions: Iterable["Transaction"],
        current_time: LogicTime,
    ) -> "SerializableTransaction":
        transasction = SerializableTransaction(
            _id=transaction_id,
            _beginning=current_time,
            _is_serializable=True,
            _effect=list(),
            _row_ids=set(),
            _marks=set(),
            _concurrent_transactions=list(active_transactions),
            _transactions_with_possible_conflict=list(),
        )

        for active_transaction in active_transactions:
            if isinstance(active_transaction, SerializableTransaction):
                active_transaction._concurrent_transactions.append(transasction)

        return transasction

    def _die(self) -> None:
        self._concurrent_transactions.clear()
        self._transactions_with_possible_conflict.clear()

    def _conflict(self) -> TransactionConflict | None:
        for transaction in self._transactions_with_possible_conflict:
            conflict_marks = frozenset(self._marks & transaction._marks)
            conflict_row_ids = self._row_ids & transaction._row_ids

            if conflict_marks or conflict_row_ids:
                return TransactionConflict(self._id, marks=conflict_marks)

        return None


@dataclass
class NonSerializableReadTransaction(Transaction):
    _id: UUID
    _beginning: LogicTime
    _is_readonly: bool

    def id(self) -> UUID:
        return self._id

    def beginning(self) -> LogicTime:
        return self._beginning

    def add_effect(self, effect: IntermediateOperatorEffect) -> None:
        if self._is_readonly and not isinstance(effect, ViewedRow):
            self._is_readonly = False

    def rollback(self) -> None:
        ...

    def commit(self) -> TransactionOkCommit | NonSerializableWriteTransaction:
        if self._is_readonly:
            return TransactionOkCommit(self._id, effect=tuple())

        return NonSerializableWriteTransaction(self._id)

    @classmethod
    def start(
        cls,
        transaction_id: UUID,
        current_time: LogicTime,
    ) -> "NonSerializableReadTransaction":
        return NonSerializableReadTransaction(
            _id=transaction_id,
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
