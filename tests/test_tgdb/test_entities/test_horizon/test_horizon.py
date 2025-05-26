from typing import Any
from uuid import UUID

from pytest import fixture, mark, raises

from tgdb.entities.horizon.effect import MutatedTuple, NewTuple
from tgdb.entities.horizon.horizon import (
    Horizon,
    InvalidTransactionStateError,
    NoTransactionError,
)
from tgdb.entities.horizon.horizon import (
    horizon as horizon_,
)
from tgdb.entities.horizon.transaction import (
    Transaction,
    TransactionCommit,
    TransactionConflict,
    TransactionIsolation,
    TransactionOkPreparedCommit,
)
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.tuple import Tuple, TupleID, tuple_


@fixture
def horizon() -> Horizon:
    return horizon_(100)


@mark.parametrize("object", ["bool", "len"])
def test_without_all(object: str, horizon: Horizon) -> None:
    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize("object", ["bool", "len"])
def test_with_only_start(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |-
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )

    if object == "bool":
        assert horizon

    if object == "len":
        assert len(horizon) == 1


@mark.parametrize("object", ["bool", "len"])
def test_two_concurrent_transactions(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--
     |-
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )
    horizon.start_transaction(
        UUID(int=2), TransactionIsolation.serializable_read_and_write
    )

    if object == "bool":
        assert horizon

    if object == "len":
        assert len(horizon) == 2


@mark.parametrize("object", ["bool", "len"])
def test_with_only_commit(
    object: str,
    horizon: Horizon,
) -> None:
    """
    -|
    """

    with raises(NoTransactionError):
        horizon.commit_transaction(UUID(int=1), [NewTuple(tuple_(1))])

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize("object", ["bool", "len"])
def test_with_only_rollback(
    object: str,
    horizon: Horizon,
) -> None:
    """
    -|
    """

    with raises(NoTransactionError):
        horizon.rollback_transaction(UUID(int=1))

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0


def test_with_two_start(horizon: Horizon) -> None:
    """
    ||-
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )

    with raises(InvalidTransactionStateError):
        horizon.start_transaction(
            UUID(int=1), TransactionIsolation.serializable_read_and_write
        )


@mark.parametrize("object", ["bool", "len"])
def test_rollback_with_transaction(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )
    horizon.rollback_transaction(UUID(int=1))

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize(
    "object",
    ["bool", "len", "commit"]
)
def test_commit_with_transaction_without_transaction_effect(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|-|
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )
    commit = horizon.commit_transaction(UUID(int=1), [])

    if object == "bool":
        assert horizon

    if object == "len":
        assert len(horizon) == 1

    if object == "commit":
        assert commit == TransactionOkPreparedCommit(UUID(int=1), set())


@mark.parametrize(
    "object",
    ["bool", "len", "commit"]
)
def test_commit_completion_with_transaction_without_transaction_effect(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|-|
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )
    commit = horizon.commit_transaction(UUID(int=1), [])
    assert isinstance(commit, TransactionOkPreparedCommit)

    commit = horizon.complete_commit(commit)

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0

    if object == "commit":
        assert commit == TransactionCommit(UUID(int=1), set())


@mark.parametrize(
    "object",
    ["bool", "len"]
)
def test_commit_completion_without_transaction(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|-|
    """

    commit = TransactionOkPreparedCommit(UUID(int=1), set())

    with raises(NoTransactionError):
        horizon.complete_commit(commit)

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize(
    "object", ["len", "prepared_commit", "completed_commit"]
)
def test_commit_with_transaction_with_effect(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|-|
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )

    prepared_commit = horizon.commit_transaction(UUID(int=1), [
        MutatedTuple(tuple_(1, "x")),
        NewTuple(tuple_(1, "y")),
    ])
    assert isinstance(prepared_commit, TransactionOkPreparedCommit)

    completed_commit = horizon.complete_commit(prepared_commit)

    if object == "len":
        assert len(horizon) == 0

    if object == "prepared_commit":
        assert prepared_commit == TransactionOkPreparedCommit(
            UUID(int=1),
            {
                MutatedTuple(tuple_(1, "y")),
            },
        )

    if object == "completed_commit":
        assert completed_commit == TransactionCommit(
            UUID(int=1),
            {MutatedTuple(tuple_(1, "y"))},
        )


@mark.parametrize(
    "object",
    [
        "len_after_commit1",
        "len_after_commit2",
        "len_after_commit3",
    ],
)
def test_horizon_movement(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--||
      |---||
         |---||
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )

    horizon.start_transaction(
        UUID(int=2), TransactionIsolation.serializable_read_and_write
    )

    commit1 = horizon.commit_transaction(UUID(int=1), [])
    assert isinstance(commit1, TransactionOkPreparedCommit)
    horizon.complete_commit(commit1)

    if object == "len_after_commit1":
        assert len(horizon) == 1

    horizon.start_transaction(
        UUID(int=3), TransactionIsolation.serializable_read_and_write
    )

    commit2 = horizon.commit_transaction(UUID(int=2), [])
    assert isinstance(commit2, TransactionOkPreparedCommit)
    horizon.complete_commit(commit2)

    if object == "len_after_commit1":
        assert len(horizon) == 1

    commit3 = horizon.commit_transaction(UUID(int=3), [])
    assert isinstance(commit3, TransactionOkPreparedCommit)
    horizon.complete_commit(commit3)

    if object == "len_after_commit1":
        assert len(horizon) == 0


@mark.parametrize(
    "object",
    ["commit1", "commit2"],
)
def test_with_sequential_transactions(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|-| |--|-|
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )
    commit = horizon.commit_transaction(UUID(int=3), [MutatedTuple(tuple_(1, "a"))])
    assert isinstance(commit, TransactionOkPreparedCommit)
    commit1 = horizon.complete_commit(commit)


    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )
    commit = horizon.commit_transaction(UUID(int=3), [MutatedTuple(tuple_(1, "b"))])
    assert isinstance(commit, TransactionOkPreparedCommit)
    commit2 = horizon.complete_commit(commit)

    if object == "commit1":
        assert commit1 == TransactionCommit(
            UUID(int=1), {MutatedTuple(tuple_(1, "a"))}
        )

    if object == "commit2":
        assert commit2 == TransactionCommit(
            UUID(int=2), {MutatedTuple(tuple_(1, "b"))}
        )


@mark.parametrize(
    "object",
    ["commit1", "commit2"],
)
def test_conflict_by_id_with_left_transaction(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |---||
       |---|
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )

    horizon.start_transaction(
        UUID(int=2), TransactionIsolation.serializable_read_and_write
    )

    commit = horizon.commit_transaction(UUID(int=1), [MutatedTuple(tuple_(1, "a"))])
    assert isinstance(commit, TransactionOkPreparedCommit)
    commit1 = horizon.complete_commit(commit)

    commit2 = horizon.commit_transaction(UUID(int=2), [MutatedTuple(tuple_(1, "b"))])

    if object == "commit1":
        assert commit1 == TransactionCommit(
            UUID(int=1), {MutatedTuple(tuple_(1, "a"))}
        )

    if object == "commit2":
        assert commit2 == TransactionConflict(UUID(int=2), frozenset())


@mark.parametrize(
    "object",
    ["bool", "len", "commit1", "commit2"],
)
def test_conflict_by_id_with_subset_transaction(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |---------||
       |---||
    """

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )

    horizon.start_transaction(
        UUID(int=2), TransactionIsolation.serializable_read_and_write
    )

    commit2 = horizon.commit_transaction(UUID(int=2), [MutatedTuple(tuple_(1, "b"))])
    assert isinstance(commit2, TransactionOkPreparedCommit)
    commit2 = horizon.complete_commit(commit2)

    commit1 = horizon.commit_transaction(UUID(int=1), [MutatedTuple(tuple_(1, "a"))])

    if object == "commit1":
        assert commit1 == TransactionConflict(UUID(int=1), frozenset())

    if object == "commit2":
        assert commit2 == TransactionCommit(
            UUID(int=2), {MutatedTuple(tuple_(1, "b"))}
        )


@mark.parametrize(
    "object",
    [
        "commit1",
        "commit2",
        "commit3",
    ],
)
def test_conflict_by_id_with_left_long_distance_transaction(
    object: str,
    horizon: Horizon,
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

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )
    horizon.start_transaction(
        UUID(int=2), TransactionIsolation.serializable_read_and_write
    )
    horizon.start_transaction(
        UUID(int=3), TransactionIsolation.serializable_read_and_write
    )

    commit = horizon.commit_transaction(UUID(int=2), [MutatedTuple(tuple_("y"))])
    assert isinstance(commit, TransactionOkPreparedCommit)
    commit2 = horizon.complete_commit(commit)

    commit = horizon.commit_transaction(UUID(int=1), [MutatedTuple(tuple_(1, "x"))])
    assert isinstance(commit, TransactionOkPreparedCommit)
    commit1 = horizon.complete_commit(commit)

    commit3 = horizon.commit_transaction(UUID(int=3), [MutatedTuple(tuple_(1, "x"))])

    if object == "commit2":
        assert commit2 == TransactionCommit(
            UUID(int=2), {MutatedTuple(tuple_("y"))}
        )

    if object == "commit1":
        assert commit1 == TransactionCommit(
            UUID(int=1), {MutatedTuple(tuple_("x"))}
        )

    if object == "commit3":
        assert commit3 == TransactionConflict(UUID(int=3), frozenset())


def test_max_len() -> None:
    """
    # |--
    #  |-
        |
    """

    horizon = horizon_(2)

    horizon.start_transaction(
        UUID(int=1), TransactionIsolation.serializable_read_and_write
    )
    assert len(horizon) == 1

    horizon.start_transaction(
        UUID(int=2), TransactionIsolation.serializable_read_and_write
    )
    assert len(horizon) == 2

    horizon.start_transaction(
        UUID(int=3), TransactionIsolation.serializable_read_and_write
    )
    assert len(horizon) == 2


def test_no_memory_leak() -> None:
    """
    |---------||
     |-------||
      |-----||
       |---||
        ...
    """

    transaction_counter = 0

    old_transaction_init = Transaction.__init__

    def new_transaction_init(*args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        old_transaction_init(*args, **kwargs)

        nonlocal transaction_counter
        transaction_counter += 1

    def transaction_del(self: Any, *args: Any, **kwargs: Any) -> None:  # noqa: ARG001,ANN401
        nonlocal transaction_counter

        assert self._id == transaction_counter
        transaction_counter -= 1

    Transaction.__init__ = new_transaction_init  # type: ignore[method-assign]
    Transaction.__del__ = transaction_del  # type: ignore[attr-defined]

    horizon = horizon_(1000)

    for xid in range(1, 101):
        horizon.start_transaction(
            UUID(int=xid), TransactionIsolation.serializable_read_and_write
        )

    assert transaction_counter == 100

    for xid in reversed(range(1, 101)):
        commit = horizon.commit_transaction(UUID(int=xid), [])
        assert isinstance(commit, TransactionOkPreparedCommit)
        horizon.complete_commit(commit)

    horizon.start_transaction(
        UUID(int=200), TransactionIsolation.serializable_read_and_write
    )

    Transaction.__init__ = old_transaction_init  # type: ignore[method-assign]
    del Transaction.__del__  # type: ignore[attr-defined]

    assert transaction_counter == 1
