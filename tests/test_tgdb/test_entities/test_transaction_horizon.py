from uuid import UUID

from pytest import fixture, mark, raises

from tgdb.entities.operator import (
    AppliedOperator,
    CommitOperator,
    RollbackOperator,
    StartOperator,
)
from tgdb.entities.row import MutatedRow, NewRow, row
from tgdb.entities.transaction import (
    TransactionConflict,
    TransactionFailedCommit,
    TransactionOkCommit,
)
from tgdb.entities.transaction_horizon import (
    NonLinearizedOperatorError,
    TransactionHorizon,
    UnattainableTransactionHorizonError,
    UnlimitedTransactionHorizonError,
    UselessMaxHeightError,
    create_transaction_horizon,
)


@fixture
def horizon() -> TransactionHorizon:
    return create_transaction_horizon(100, 100)


@mark.parametrize("object", ["bool", "begining", "time", "height", "width"])
def test_without_all(object: str, horizon: TransactionHorizon) -> None:
    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() is None

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0


@mark.parametrize("object", ["bool", "begining", "time", "height", "width"])
def test_with_only_start_operator(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |-
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))

    if object == "bool":
        assert horizon

    if object == "begining":
        assert horizon.beginning() == 1

    if object == "time":
        assert horizon.time() == 1

    if object == "height":
        assert horizon.height() == 1

    if object == "width":
        assert horizon.width() == 1


@mark.parametrize("object", ["bool", "begining", "time", "height", "width"])
def test_two_concurrent_transactions(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |--
     |-
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))
    horizon.add(AppliedOperator(StartOperator(UUID(int=2)), 2))

    if object == "bool":
        assert horizon

    if object == "begining":
        assert horizon.beginning() == 1

    if object == "time":
        assert horizon.time() == 2

    if object == "height":
        assert horizon.height() == 2

    if object == "width":
        assert horizon.width() == 2


@mark.parametrize(
    "object", ["bool", "begining", "time", "height", "width", "commit"]
)
def test_with_only_commit_operator(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    -|
    """

    commit = horizon.add(
        AppliedOperator(CommitOperator(UUID(int=1), [NewRow(row(1))]), 1)
    )

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 1

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0

    if object == "commit":
        assert commit == TransactionFailedCommit(UUID(int=1), None)


@mark.parametrize("object", ["bool", "begining", "time", "height", "width"])
def test_with_only_rollback_operator(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    -|
    """

    horizon.add(AppliedOperator(RollbackOperator(UUID(int=1)), 1))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 1

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0


def test_with_parallel_operator(horizon: TransactionHorizon) -> None:
    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))

    with raises(NonLinearizedOperatorError):
        horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))


def test_with_operator_from_past(horizon: TransactionHorizon) -> None:
    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))

    with raises(NonLinearizedOperatorError):
        horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 0))


@mark.parametrize("object", ["bool", "begining", "time", "height", "width"])
def test_rollback_with_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |--|
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))
    horizon.add(AppliedOperator(RollbackOperator(UUID(int=1)), 20))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 20

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0


@mark.parametrize(
    "object", ["bool", "begining", "time", "height", "width", "commit"]
)
def test_commit_with_transaction_without_intermediate_operators(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |--|
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))
    commit = horizon.add(AppliedOperator(CommitOperator(UUID(int=1), []), 10))

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 10

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0

    if object == "commit":
        assert commit == TransactionOkCommit(UUID(int=1), [], 10)


@mark.parametrize(
    "object", ["bool", "begining", "time", "height", "width", "commit"]
)
def test_commit_with_transaction_with_ok_intermediate_operators(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |--|
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))
    commit = horizon.add(
        AppliedOperator(
            CommitOperator(
                UUID(int=1),
                [MutatedRow(row(1, "x"), None), NewRow(row(1, "y"))],
            ),
            10,
        )
    )

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 10

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0

    if object == "commit":
        assert commit == TransactionOkCommit(
            UUID(int=1),
            [MutatedRow(row(1, "x"), None), NewRow(row(1, "y"))],
            10,
        )


@mark.parametrize(
    "object",
    [
        "bool_after_commit1",
        "begining_after_commit1",
        "time_after_commit1",
        "height_after_commit1",
        "width_after_commit1",
        "bool_after_commit2",
        "begining_after_commit2",
        "time_after_commit2",
        "height_after_commit2",
        "width_after_commit2",
        "bool_after_commit3",
        "begining_after_commit3",
        "time_after_commit3",
        "width_after_commit3",
    ],
)
def test_horizon_movement(  # noqa: PLR0912
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |--|
      |--|
        |--|
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), -100))
    horizon.add(AppliedOperator(StartOperator(UUID(int=2)), -50))
    horizon.add(AppliedOperator(CommitOperator(UUID(int=1), []), -25))

    if object == "bool_after_commit1":
        assert horizon

    if object == "begining_after_commit1":
        assert horizon.beginning() == -50

    if object == "time_after_commit1":
        assert horizon.time() == -25

    if object == "height_after_commit1":
        assert horizon.height() == 1

    if object == "width_after_commit1":
        assert horizon.width() == 26

    horizon.add(AppliedOperator(StartOperator(UUID(int=3)), -10))
    horizon.add(AppliedOperator(CommitOperator(UUID(int=2), []), -5))

    if object == "bool_after_commit2":
        assert horizon

    if object == "begining_after_commit2":
        assert horizon.beginning() == -10

    if object == "time_after_commit2":
        assert horizon.time() == -5

    if object == "height_after_commit2":
        assert horizon.height() == 1

    if object == "width_after_commit2":
        assert horizon.width() == 6

    horizon.add(AppliedOperator(CommitOperator(UUID(int=3), []), 150))

    if object == "bool_after_commit3":
        assert not horizon

    if object == "begining_after_commit3":
        assert horizon.beginning() is None

    if object == "time_after_commit3":
        assert horizon.time() == 150

    if object == "height_after_commit3":
        assert horizon.height() == 0

    if object == "width_after_commit3":
        assert horizon.width() == 0


@mark.parametrize(
    "object",
    ["bool", "begining", "time", "height", "width", "commit1", "commit2"],
)
def test_with_sequential_transactions(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |---| |---|
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 0))
    commit1 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=1), [MutatedRow(row(1, "a"), None)]),
            2,
        )
    )

    horizon.add(AppliedOperator(StartOperator(UUID(int=2)), 10))
    commit2 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=2), [MutatedRow(row(1, "b"), None)]),
            11,
        )
    )

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 11

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0

    if object == "commit1":
        assert commit1 == TransactionOkCommit(
            UUID(int=1), [MutatedRow(row(1, "a"), None)], 2
        )

    if object == "commit2":
        assert commit2 == TransactionOkCommit(
            UUID(int=2), [MutatedRow(row(1, "b"), None)], 11
        )


@mark.parametrize(
    "object",
    ["bool", "begining", "time", "height", "width", "commit1", "commit2"],
)
def test_conflict_by_id_with_left_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |---|
       |---|
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 0))

    horizon.add(AppliedOperator(StartOperator(UUID(int=2)), 1))

    commit1 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=1), [MutatedRow(row(1, "a"), None)]),
            2,
        )
    )

    commit2 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=2), [MutatedRow(row(1, "b"), None)]),
            3,
        )
    )

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 3

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0

    if object == "commit1":
        assert commit1 == TransactionOkCommit(
            UUID(int=1), [MutatedRow(row(1, "a"), None)], 2
        )

    if object == "commit2":
        assert commit2 == TransactionFailedCommit(
            UUID(int=2), TransactionConflict(frozenset())
        )


@mark.parametrize(
    "object",
    ["bool", "begining", "time", "height", "width", "commit1", "commit2"],
)
def test_conflict_by_id_with_subset_transaction(
    object: str,
    horizon: TransactionHorizon,
) -> None:
    """
    |---------|
       |---|
    """

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 0))

    horizon.add(AppliedOperator(StartOperator(UUID(int=2)), 1))

    commit2 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=2), [MutatedRow(row(1, "b"), None)]),
            2,
        )
    )

    commit1 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=1), [MutatedRow(row(1, "a"), None)]),
            3,
        )
    )

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 3

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0

    if object == "commit1":
        assert commit1 == TransactionFailedCommit(
            UUID(int=1), TransactionConflict(frozenset())
        )

    if object == "commit2":
        assert commit2 == TransactionOkCommit(
            UUID(int=2), [MutatedRow(row(1, "b"), None)], 2
        )


@mark.parametrize(
    "object",
    [
        "bool",
        "begining",
        "time",
        "height",
        "width",
        "commit1",
        "commit2",
        "commit3",
    ],
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

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 0))
    horizon.add(AppliedOperator(StartOperator(UUID(int=2)), 1))
    horizon.add(AppliedOperator(StartOperator(UUID(int=3)), 3))

    commit2 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=2), [MutatedRow(row("y"), None)]),
            4,
        )
    )
    commit1 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=1), [MutatedRow(row("x"), None)]),
            5,
        )
    )
    commit3 = horizon.add(
        AppliedOperator(
            CommitOperator(UUID(int=3), [MutatedRow(row("x"), None)]),
            6,
        )
    )

    if object == "bool":
        assert not horizon

    if object == "begining":
        assert horizon.beginning() is None

    if object == "time":
        assert horizon.time() == 6

    if object == "height":
        assert horizon.height() == 0

    if object == "width":
        assert horizon.width() == 0

    if object == "commit1":
        assert commit1 == TransactionOkCommit(
            UUID(int=1), [MutatedRow(row("x"), None)], 5
        )

    if object == "commit2":
        assert commit2 == TransactionOkCommit(
            UUID(int=2), [MutatedRow(row("y"), None)], 4
        )

    if object == "commit3":
        assert commit3 == TransactionFailedCommit(
            UUID(int=3), TransactionConflict(frozenset())
        )


def test_max_width() -> None:
    """
    ##
    |--
     |-
      |
    """

    horizon = create_transaction_horizon(2, None)

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))

    assert horizon.width() == 1
    assert horizon.height() == 1
    assert horizon.beginning() == 1

    horizon.add(AppliedOperator(StartOperator(UUID(int=2)), 2))

    assert horizon.width() == 2
    assert horizon.height() == 2
    assert horizon.beginning() == 1

    horizon.add(AppliedOperator(StartOperator(UUID(int=3)), 3))

    assert horizon.width() == 2
    assert horizon.height() == 2
    assert horizon.beginning() == 2


def test_max_height() -> None:
    """
    # |--
    #  |-
        |
    """

    horizon = create_transaction_horizon(None, 2)

    horizon.add(AppliedOperator(StartOperator(UUID(int=1)), 1))

    assert horizon.width() == 1
    assert horizon.height() == 1
    assert horizon.beginning() == 1

    horizon.add(AppliedOperator(StartOperator(UUID(int=2)), 2))

    assert horizon.width() == 2
    assert horizon.height() == 2
    assert horizon.beginning() == 1

    horizon.add(AppliedOperator(StartOperator(UUID(int=3)), 3))

    assert horizon.width() == 2
    assert horizon.height() == 2
    assert horizon.beginning() == 2


def test_unlimited_horizon() -> None:
    with raises(UnlimitedTransactionHorizonError):
        create_transaction_horizon(None, None)


def test_unattainable_horizon_by_width() -> None:
    with raises(UnattainableTransactionHorizonError):
        create_transaction_horizon(0, None)


def test_unattainable_horizon_by_height() -> None:
    with raises(UnattainableTransactionHorizonError):
        create_transaction_horizon(None, 0)


def test_useless_max_height() -> None:
    with raises(UselessMaxHeightError):
        create_transaction_horizon(10, 15)


def test_no_errors_on_square_size_limit() -> None:
    create_transaction_horizon(10, 10)
