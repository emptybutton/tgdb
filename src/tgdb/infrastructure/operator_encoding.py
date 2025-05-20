from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from tgdb.entities.mark import Mark
from tgdb.entities.operator import (
    CommitOperator,
    IntermediateOperator,
    Operator,
    RollbackOperator,
    StartOperator,
)
from tgdb.entities.row import (
    DeletedRow,
    MutatedRow,
    NewRow,
    Row,
    RowAttribute,
    row,
)
from tgdb.infrastructure.primitive_encoding import (
    decoded_primitive,
    decoded_str,
    decoded_uuid,
    encoded_primitive,
)


encoded_primitive_type_by_type = {
    type(None): "n",
    bool: "b",
    int: "i",
    str: "s",
    datetime: "d",
    UUID: "u",
}
type_by_encoded_primitive_type = dict(zip(
    encoded_primitive_type_by_type.values(),
    encoded_primitive_type_by_type.keys(),
    strict=True,
))


def encoded_start_operator(operator: StartOperator) -> str:
    return f"s{encoded_primitive(operator.transaction_id)}"


def encoded_rollback_operator(operator: RollbackOperator) -> str:
    return f"r{encoded_primitive(operator.transaction_id)}"


def encoded_commit_operator(operator: CommitOperator) -> str:
    return f"c{encoded_primitive(operator.transaction_id)}"


def encoded_commit_intermediate_operators(
    commit_operator: CommitOperator
) -> Iterable[str]:
    return (
        encoded_intermediate_operator(operator, commit_operator.transaction_id)
        for operator in commit_operator.operators
    )


def encoded_intermediate_operator(
    intermediate_operator: IntermediateOperator, transaction_id: UUID
) -> str:
    match intermediate_operator:
        case NewRow(row):
            operation_header = "N"
        case MutatedRow(row):
            operation_header = "M"
        case DeletedRow(row_id):
            operation_header = "D"
        case Mark() as mark:
            operation_header = "K"

    match intermediate_operator:
        case NewRow(row) | MutatedRow(row):
            body = encoded_operator_row(row)
        case DeletedRow(row_id):
            body = encoded_operator_row_attribute(row_id)
        case Mark() as mark:
            body = (
                f"{encoded_primitive(mark.id)}|{encoded_primitive(mark.key)}"
            )

    return f"{operation_header}{encoded_primitive(transaction_id)}:{body}"


def encoded_operator_row(row: Row) -> str:
    values = list[str]()
    values.append(encoded_primitive(row.schema))
    values.extend(map(encoded_operator_row_attribute, row))

    return "|".join(values)


def encoded_operator_row_attribute(attribute: RowAttribute) -> str:
    return (
        f"{encoded_primitive_type_by_type[type(attribute)]}"
        f"{encoded_primitive(attribute)}"
    )


def decoded_operator_row_attribute(encoded_attribute: str) -> RowAttribute:
    type_ = type_by_encoded_primitive_type[encoded_attribute[0]]

    return decoded_primitive(encoded_attribute[1:], type_)


def decoded_operator_row(encoded_row: str) -> Row:
    values = encoded_row.split("|")

    schema = decoded_str(values[0])
    attrs = map(decoded_operator_row_attribute, values[1:])

    return row(*attrs, schema=schema)


@dataclass(frozen=True)
class DecodedIntermediateOperator:
    operator: IntermediateOperator
    transaction_id: UUID


def decoded_operator(
    encoded_operator: str,
    intermediate_operators_by_transactoin_id: dict[UUID, list[IntermediateOperator]]
) -> Operator | DecodedIntermediateOperator:
    head = encoded_operator[0]
    tail = encoded_operator[1:]

    match head:
        case "s":
            return StartOperator(decoded_uuid(tail))

        case "r":
            return RollbackOperator(decoded_uuid(tail))

        case "c":
            transaction_id = decoded_uuid(tail)

            intermediate_operators = (
                intermediate_operators_by_transactoin_id
                .get(transaction_id, list())
            )
            return CommitOperator(transaction_id, intermediate_operators)

    encoded_transaction_id, body = tail.split(":")
    transaction_id = decoded_uuid(encoded_transaction_id)

    match head:
        case "N":
            operator = NewRow(decoded_operator_row(body))
        case "M":
            operator = MutatedRow(decoded_operator_row(body), None)
        case "D":
            operator = DeletedRow(decoded_operator_row_attribute(body), None)
        case "K":
            id, key = tail.split("|")
            operator = Mark(decoded_uuid(id), decoded_str(key))
        case _:
            raise ValueError

    return DecodedIntermediateOperator(operator, transaction_id)
