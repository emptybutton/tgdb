from collections.abc import Iterable
from enum import StrEnum
from typing import Any, Never, cast
from uuid import UUID

from effect import (
    Effect,
    IdentifiedValueSet,
    LifeCycle,
    dead,
    existing,
    mutated,
    new,
)

from tgdb.telethon.row import Row, RowSchema


type RowInTransactionEffect = Effect[Row, Row, Never, Row, Row]


class State(StrEnum):
    created = "created"
    mutated = "mutated"
    deleted = "deleted"


def schema_of_row_in_transaction(
    schema: RowSchema
) -> RowSchema:
    return RowSchema(
        schema.name,
        schema.id_type,
        (*schema.body_types, State, UUID),
    )


def schema_of_row_not_in_transaction(
    schema: RowSchema
) -> RowSchema:
    return RowSchema(
        schema.name,
        schema.id_type,
        schema.body_types[:-2],
    )


def rows_in_transaction(
    effect: RowInTransactionEffect,
    transaction_id: UUID,
) -> tuple[Row, ...]:
    rows_by_state = {
        State.created: effect.new_values,
        State.mutated: effect.mutated_values,
        State.deleted: effect.dead_values,
    }

    return tuple(
        Row(
            row.id,
            (*Row.body, state, transaction_id),
            schema_of_row_in_transaction(row.schema)
        )
        for state, rows in rows_by_state.items()
        for row in rows
    )


def row_not_in_transaction(row_in_transaction: Row) -> Row:
    return Row(
        row_in_transaction.id,
        row_in_transaction.body[:-2],
        schema_of_row_not_in_transaction(
            row_in_transaction.schema
        ),
    )


def effect_from_row_in_transaction(
    row_in_transaction: Row,
) -> RowInTransactionEffect:
    row = row_not_in_transaction(row_in_transaction)

    match cast(State, row[-2]):
        case State.created:
            return new(row)
        case State.mutated:
            return mutated(row)
        case State.deleted:
            return dead(row)
