from abc import ABC, abstractmethod
from collections.abc import Iterable
from itertools import chain
from uuid import UUID

from pydantic import BaseModel, Field

from tgdb.entities.mark import Mark
from tgdb.entities.message import Message
from tgdb.entities.operator import (
    CommitOperator,
    IntermediateOperator,
    RollbackOperator,
    StartOperator,
)
from tgdb.entities.row import (
    DeletedRow,
    MutatedRow,
    NewRow,
    Row,
    RowAttribute,
    ViewedRow,
)


class EntitySchema[EntityT](BaseModel, ABC):
    @abstractmethod
    def entity(self) -> EntityT: ...


class RowSchema(EntitySchema[Row]):
    id: RowAttribute
    body: tuple[RowAttribute, ...]
    schema_: str = Field(alias="schema")

    def entity(self) -> Row:
        return Row(self.id, self.body, self.schema_)


class MessageSchema(EntitySchema[Message]):
    id: int
    author_id: int = Field(alias="authorID")

    def entity(self) -> Message:
        return Message(self.id, self.author_id)


class NewRowOperatorSchema(EntitySchema[NewRow]):
    row: RowSchema

    def entity(self) -> NewRow:
        return NewRow(self.row.entity())


class MutatedRowOperatorSchema(EntitySchema[MutatedRow]):
    row: RowSchema
    row_message: MessageSchema | None = Field(alias="rowMessage")

    def entity(self) -> MutatedRow:
        message = self.row_message.entity() if self.row_message else None

        return MutatedRow(self.row.entity(), message)


class DeletedRowOperatorSchema(EntitySchema[DeletedRow]):
    row_id: RowAttribute = Field(alias="rowID")
    row_message: MessageSchema | None = Field(alias="rowMessage")

    def entity(self) -> DeletedRow:
        message = self.row_message.entity() if self.row_message else None

        return DeletedRow(self.row_id, message)


class ViewedRowOperatorSchema(EntitySchema[ViewedRow]):
    row_id: RowAttribute = Field(alias="rowID")

    def entity(self) -> ViewedRow:
        return ViewedRow(self.row_id)


class MarkOperatorSchema(EntitySchema[Mark]):
    id: UUID
    key: str

    def entity(self) -> Mark:
        return Mark(self.id, self.key)

    @classmethod
    def of(cls, mark: Mark) -> "MarkOperatorSchema":
        return MarkOperatorSchema(id=mark.id, key=mark.key)


class StartOperatorSchema(EntitySchema[StartOperator]):
    transaction_id: UUID = Field(alias="transactionID")

    def entity(self) -> StartOperator:
        return StartOperator(self.transaction_id)


class RollbackOperatorSchema(EntitySchema[RollbackOperator]):
    transaction_id: UUID = Field(alias="transactionID")

    def entity(self) -> RollbackOperator:
        return RollbackOperator(self.transaction_id)


class CommitOperatorSchema(EntitySchema[CommitOperator]):
    transaction_id: UUID = Field(alias="transactionID")

    new_row_operators: tuple[NewRowOperatorSchema, ...] = Field(
        alias="newRowOperators",
    )
    mutated_row_operators: tuple[MutatedRowOperatorSchema, ...] = Field(
        alias="mutatedRowOperators",
    )
    deleted_row_operators: tuple[DeletedRowOperatorSchema, ...] = Field(
        alias="deletedRowOperators",
    )
    viewed_row_operators: tuple[ViewedRowOperatorSchema, ...] = Field(
        alias="viewedRowOperators",
    )

    mark_operators: tuple[MarkOperatorSchema, ...] = Field(
        alias="markOperators"
    )

    def entity(self) -> CommitOperator:
        return CommitOperator(self.transaction_id, self._entity_operators())

    def _entity_operators(self) -> tuple[IntermediateOperator, ...]:
        schema_operators: Iterable[EntitySchema[IntermediateOperator]] = chain(
            self.new_row_operators,
            self.mutated_row_operators,
            self.deleted_row_operators,
            self.mark_operators,
        )

        return tuple(
            schema_operator.entity() for schema_operator in schema_operators
        )
