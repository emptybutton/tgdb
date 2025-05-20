from datetime import UTC, datetime
from uuid import UUID

from pytest import mark

from tgdb.entities.mark import Mark
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
    row,
)
from tgdb.infrastructure.operator_encoding import (
    DecodedIntermediateOperator,
    decoded_operator,
    decoded_operator_row,
    decoded_operator_row_attribute,
    encoded_commit_intermediate_operators,
    encoded_commit_operator,
    encoded_operator_row,
    encoded_operator_row_attribute,
    encoded_rollback_operator,
    encoded_start_operator,
)


@mark.parametrize("attr, encoded_attr", [
    (None, "n^"),
    (True, "b1"),
    (False, "b0"),
    (12345, "i12345"),
    ("asdasd", "sasdasd"),
    ("#asd+", r"s%23asd%2B"),
    ("#asd+", r"s%23asd%2B"),
    (UUID(int=0), "u00000000000000000000000000000000"),
    (datetime(2000, 1, 1, tzinfo=UTC), "d2000-01-01T00:00:00+00:00"),
])
def test_operator_row_attribute_encoding(
    attr: RowAttribute, encoded_attr: str
) -> None:
    assert encoded_operator_row_attribute(attr) == encoded_attr
    assert decoded_operator_row_attribute(encoded_attr) == attr


@mark.parametrize("row, encoded_row", [
    (row(1, 1), "__undefined__|i1|i1"),
    (row(None, schema="x"), "x|n^"),
    (row(True, True, False, schema="#asd+"), "%23asd%2B|b1|b1|b0"),
])
def test_operator_row_encoding(
    row: Row, encoded_row: str
) -> None:
    assert encoded_operator_row(row) == encoded_row
    assert decoded_operator_row(encoded_row) == row


@mark.parametrize("operator, encoded_operator_", [
    (StartOperator(UUID(int=0)), "s00000000000000000000000000000000"),
])
def test_start_operator_encoding_without(
    operator: StartOperator, encoded_operator_: str
) -> None:
    assert encoded_start_operator(operator) == encoded_operator_
    assert decoded_operator(encoded_operator_, {}) == operator


@mark.parametrize("operator, encoded_operator_", [
    (RollbackOperator(UUID(int=0)), "r00000000000000000000000000000000"),
])
def test_rollback_operator_encoding(
    operator: RollbackOperator, encoded_operator_: str
) -> None:
    assert encoded_rollback_operator(operator) == encoded_operator_
    assert decoded_operator(encoded_operator_, {}) == operator


def test_encoded_commit_intermediate_operators_without_operators() -> None:
    operator = CommitOperator(UUID(int=0), [])

    assert list(encoded_commit_intermediate_operators(operator)) == []


def test_encoded_commit_intermediate_operators_with_operators() -> None:
    operator = CommitOperator(
        UUID(int=0),
        [NewRow(row(1, True, schema="#")), Mark(UUID(int=1), "#asd+")],
    )

    assert list(encoded_commit_intermediate_operators(operator)) == [
        "N00000000000000000000000000000000:%23|i1|b1",
        "K00000000000000000000000000000000:00000000000000000000000000000001|%23asd%2B",
    ]


def test_commit_intermediate_operators_with_operators() -> None:
    operator = CommitOperator(
        UUID(int=0),
        [NewRow(row(1, True, schema="#")), Mark(UUID(int=1), "#asd+")],
    )

    assert list(encoded_commit_intermediate_operators(operator)) == [
        "N00000000000000000000000000000000:%23|i1|b1",
        "K00000000000000000000000000000000:00000000000000000000000000000001|%23asd%2B",
    ]


@mark.parametrize("operator, encoded_operator_", [
    (
        CommitOperator(UUID(int=0), []),
        "c00000000000000000000000000000000",
    ),
])
def test_commit_operator_encoding_without_intermediate_operators(
    operator: CommitOperator, encoded_operator_: str
) -> None:
    assert encoded_commit_operator(operator) == encoded_operator_
    assert decoded_operator(encoded_operator_, {}) == operator


def test_decoded_commit_operator_with_intermediate_operators() -> None:
    operator_map: dict[UUID, list[IntermediateOperator]] = {
        UUID(int=0): [
            NewRow(row(1, True, schema="#")),
            Mark(UUID(int=1), "#asd+"),
        ]
    }

    result = decoded_operator(
        "c00000000000000000000000000000000", operator_map
    )

    expected_result = CommitOperator(
        UUID(int=0),
        [NewRow(row(1, True, schema="#")), Mark(UUID(int=1), "#asd+")],
    )
    assert result == expected_result


@mark.parametrize("operator, encoded_operator_", [
    (
        DecodedIntermediateOperator(
            NewRow(row(1, True, schema="#")), UUID(int=0)
        ),
        "N00000000000000000000000000000000:%23|i1|b1",
    ),
    (
        DecodedIntermediateOperator(
            MutatedRow(row(1, True, schema="#"), None), UUID(int=0)
        ),
        "M00000000000000000000000000000000:%23|i1|b1",
    ),
    (
        DecodedIntermediateOperator(DeletedRow(None, None), UUID(int=0)),
        "D00000000000000000000000000000000:n^",
    ),
])
def test_decoded_intermediate_operator(
    operator: DecodedIntermediateOperator,
    encoded_operator_: str
) -> None:
    result = decoded_operator(encoded_operator_, {})

    assert result == operator
