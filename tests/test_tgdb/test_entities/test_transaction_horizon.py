from uuid import UUID

from pytest import fixture, mark, raises

from tgdb.entities.operator import (
    AppliedOperator,
    MutatedRow,
    NewRow,
    TransactionState,
)
from tgdb.entities.row import row
from tgdb.entities.transaction import (
    TransactionConflict,
    TransactionFailedCommit,
    TransactionOkCommit,
)
from tgdb.entities.transaction_horizon import (
    NonLinearizedOperatorError,
    TransactionHorizon,
    create_transaction_horizon,
)


@fixture
def horizon() -> TransactionHorizon:
    return create_transaction_horizon(100)


@mark.parametrize("object", ["bool", "begining", "time", "len"])
def test_without_all(object: str, horizon: TransactionHorizon) -> None:
    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() is None

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize("object", ["bool", "begining", "time", "len"])
def test_with_one_bad_operator(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    operator = AppliedOperator(NewRow(row(1, "a")), UUID(int=1), 1)
    horizon.add(operator)

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 1

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize("object", ["bool", "begining", "time", "len"])
def test_with_one_ok_operator(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    operator = AppliedOperator(TransactionState.started, UUID(int=1), 1)
    horizon.add(operator)

    if object == "bool":
        assert horizon

    if object == "begining":
        assert horizon.beginning() == 1

    if object == "time":
        assert horizon.time() == 1

    if object == "len":
        assert len(horizon) == 1


@mark.parametrize("object", ["bool", "begining", "time", "len"])
def test_with_two_ok_operators(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        1,
    ))
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=2),
        2,
    ))

    if object == "bool":
        assert horizon

    if object == "begining":
        assert horizon.beginning() == 1

    if object == "time":
        assert horizon.time() == 2

    if object == "len":
        assert len(horizon) == 2


def test_with_parallel_operators(horizon: TransactionHorizon) -> None:
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        1,
    ))

    with raises(NonLinearizedOperatorError):
        horizon.add(AppliedOperator(
            TransactionState.started,
            UUID(int=1),
            1,
        ))


def test_with_operator_from_past(horizon: TransactionHorizon) -> None:
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        1,
    ))

    with raises(NonLinearizedOperatorError):
        horizon.add(AppliedOperator(
            TransactionState.started,
            UUID(int=0),
            0,
        ))


@mark.parametrize("object", ["bool", "begining", "time", "len"])
def test_rollback_with_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        1,
    ))
    horizon.add(AppliedOperator(
        TransactionState.rollbacked,
        UUID(int=1),
        20,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 20

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize("object", ["bool", "begining", "time", "len"])
def test_rollback_without_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    horizon.add(AppliedOperator(
        TransactionState.rollbacked,
        UUID(int=1),
        1,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 1

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize("object", ["bool", "begining", "time", "len", "commit"])
def test_commit_without_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    commit = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        10,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 10

    if object == "len":
        assert len(horizon) == 0

    if object == "commit":
        assert commit == TransactionFailedCommit(UUID(int=1), conflict=None)


@mark.parametrize("object", ["bool", "begining", "time", "len", "commit"])
def test_commit_with_transaction_without_intermediate_operators(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        1,
    ))
    commit = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        10,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 10

    if object == "len":
        assert len(horizon) == 0

    if object == "commit":
        assert commit == TransactionOkCommit(UUID(int=1), [], 10)


@mark.parametrize("object", ["bool", "begining", "time", "len", "commit"])
def test_commit_with_transaction_with_ok_intermediate_operators(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        1,
    ))
    horizon.add(AppliedOperator(
        NewRow(row(1, "a")),
        UUID(int=1),
        2,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row(1, "b")),
        UUID(int=1),
        5,
    ))
    commit = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        10,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 10

    if object == "len":
        assert len(horizon) == 0

    if object == "commit":
        assert commit == TransactionOkCommit(
            UUID(int=1), [NewRow(row(1, "a")), MutatedRow(row(1, "b"))], 10
        )


@mark.parametrize("object", ["bool", "begining", "time", "len", "commit"])
def test_commit_with_transaction_with_bad_intermediate_operators(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        1,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row(1, "b")),
        UUID(int=1),
        5,
    ))
    horizon.add(AppliedOperator(
        NewRow(row(1, "a")),
        UUID(int=1),
        6,
    ))
    commit = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        10,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 10

    if object == "len":
        assert len(horizon) == 0

    if object == "commit":
        assert commit == TransactionOkCommit(
            UUID(int=1),
            [MutatedRow(row(1, "b")), NewRow(row(1, "a"))],
            10,
        )


@mark.parametrize(
    "object",
    [
        "bool_after_commit1",
        "begining_after_commit1",
        "time_after_commit1",
        "len_after_commit1",

        "bool_after_commit2",
        "begining_after_commit2",
        "time_after_commit2",
        "len_after_commit2",

        "bool_after_commit3",
        "begining_after_commit3",
        "time_after_commit3",
        "len_after_commit3",
    ]
)
def test_horizon_movement(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |--|
      |--|
        |--|
    """

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        -100,
    ))
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=2),
        -50,
    ))
    horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        0,
    ))

    if object == "bool_after_commit1":
        assert horizon

    if object == "begining_after_commit1":
        assert horizon.beginning() == -50

    if object == "time_after_commit1":
        assert horizon.time() == 0

    if object == "len_after_commit1":
        assert len(horizon) == 1

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=3),
        50,
    ))
    horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=2),
        100,
    ))

    if object == "bool_after_commit2":
        assert horizon

    if object == "begining_after_commit2":
        assert horizon.beginning() == 50

    if object == "time_after_commit2":
        assert horizon.time() == 100

    if object == "len_after_commit2":
        assert len(horizon) == 1

    horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=3),
        150,
    ))

    if object == "bool_after_commit3":
        assert not horizon

    if object == "begining_after_commit3":
        assert horizon.beginning() is None

    if object == "time_after_commit3":
        assert horizon.time() == 150

    if object == "len_after_commit3":
        assert len(horizon) == 0


@mark.parametrize(
    "object", ["bool", "begining", "time", "len", "commit1", "commit2"]
)
def test_with_sequential_transactions(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |-|-| |-|-|
    """

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        0,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row(1, "a")),
        UUID(int=1),
        1,
    ))
    commit1 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        2,
    ))

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=2),
        10,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row(1, "b")),
        UUID(int=2),
        11,
    ))
    commit2 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=2),
        12,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 12

    if object == "len":
        assert len(horizon) == 0

    if object == "commit1":
        assert commit1 == TransactionOkCommit(
            UUID(int=1), [MutatedRow(row(1, "a"))], 2
        )

    if object == "commit2":
        assert commit2 == TransactionOkCommit(
            UUID(int=2), [MutatedRow(row(1, "b"))], 12
        )


@mark.parametrize(
    "object", ["bool", "begining", "time", "len", "commit1", "commit2"]
)
def test_conflict_by_id_with_left_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |-|-|
       |-|-|
    """

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        0,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row(1, "a")),
        UUID(int=1),
        1,
    ))

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=2),
        2,
    ))

    commit1 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        3,
    ))

    horizon.add(AppliedOperator(
        MutatedRow(row(1, "b")),
        UUID(int=2),
        4,
    ))
    commit2 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=2),
        5,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 5

    if object == "len":
        assert len(horizon) == 0

    if object == "commit1":
        assert commit1 == TransactionOkCommit(
            UUID(int=1), [MutatedRow(row(1, "a"))], 3
        )

    if object == "commit2":
        assert commit2 == TransactionFailedCommit(
            UUID(int=2), TransactionConflict(frozenset())
        )


@mark.parametrize(
    "object", ["bool", "begining", "time", "len", "commit1", "commit2"]
)
def test_conflict_by_id_with_subset_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |-|-------|
       |-|-|
    """

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        0,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row(1, "a")),
        UUID(int=1),
        1,
    ))

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=2),
        2,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row(1, "b")),
        UUID(int=2),
        3,
    ))
    commit2 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=2),
        4,
    ))
    commit1 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        5,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 5

    if object == "len":
        assert len(horizon) == 0

    if object == "commit1":
        assert commit1 == TransactionFailedCommit(
            UUID(int=1), TransactionConflict(frozenset())
        )

    if object == "commit2":
        assert commit2 == TransactionOkCommit(
            UUID(int=2), [MutatedRow(row(1, "b"))], 4
        )


@mark.parametrize(
    "object", ["bool", "begining", "time", "len", "commit1", "commit2"]
)
def test_conflict_by_id_with_left_long_distance_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |---x-|
     |-y-|
      |----x-|

    Begin order: 123
    Commit order: 213

    1 mutates x
    2 mutates y
    3 mutates x
    """

    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=1),
        0,
    ))
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=2),
        1,
    ))
    horizon.add(AppliedOperator(
        TransactionState.started,
        UUID(int=3),
        2,
    ))

    horizon.add(AppliedOperator(
        MutatedRow(row("y")),
        UUID(int=2),
        3,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row("x")),
        UUID(int=1),
        4,
    ))
    commit2 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=2),
        5,
    ))
    commit1 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=1),
        6,
    ))
    horizon.add(AppliedOperator(
        MutatedRow(row("x")),
        UUID(int=3),
        7,
    ))
    commit3 = horizon.add(AppliedOperator(
        TransactionState.committed,
        UUID(int=3),
        8,
    ))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 8

    if object == "len":
        assert len(horizon) == 0

    if object == "commit1":
        assert commit1 == TransactionOkCommit(
            UUID(int=1), [MutatedRow(row("x"))], 6
        )

    if object == "commit2":
        assert commit2 == TransactionOkCommit(
            UUID(int=2), [MutatedRow(row("y"))], 5
        )

    if object == "commit3":
        assert commit3 == TransactionFailedCommit(
            UUID(int=3), TransactionConflict(frozenset())
        )


# def test_max_len(horizon: TransactionHorizon) -> None:
#     """
#     ######
#     |-----|
#     """
