from time import sleep
from typing import Any
from uuid import UUID

from pytest import fixture, mark, raises

from tgdb.entities.horizon.horizon import (
    Horizon,
    InvalidTransactionStateError,
    NoTransactionError,
)
from tgdb.entities.horizon.horizon import (
    horizon as horizon_,
)
from tgdb.entities.horizon.transaction import (
    Commit,
    ConflictError,
    IsolationLevel,
    PreparedCommit,
    SerializableTransaction,
    Transaction,
)
from tgdb.entities.relation.tuple import tuple_
from tgdb.entities.relation.tuple_effect import MutatedTuple, NewTuple


@fixture
def horizon() -> Horizon:
    return horizon_(0, 1000, 1_000_000_000)


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
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
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
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )
    horizon.start_transaction(
        2, UUID(int=2), IsolationLevel.serializable_read_and_write
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
        horizon.commit_transaction(
            1, UUID(int=1), [NewTuple(tuple_(tid=UUID(int=0)))]
        )

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
        horizon.rollback_transaction(1, UUID(int=1))

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0


def test_with_two_start(horizon: Horizon) -> None:
    """
    ||-
    """

    horizon.start_transaction(
        2, UUID(int=1), IsolationLevel.serializable_read_and_write
    )

    with raises(InvalidTransactionStateError):
        horizon.start_transaction(
            3, UUID(int=1), IsolationLevel.serializable_read_and_write
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
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )
    horizon.rollback_transaction(2, UUID(int=1))

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize("object", ["bool", "len", "commit"])
def test_commit_with_transaction_without_transaction_effect(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|-|
    """

    horizon.start_transaction(
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )
    commit = horizon.commit_transaction(2, UUID(int=1), [])

    if object == "bool":
        assert horizon

    if object == "len":
        assert len(horizon) == 1

    if object == "commit":
        assert commit == PreparedCommit(UUID(int=1), set())


@mark.parametrize("object", ["bool", "len", "commit"])
def test_commit_completion_with_transaction_without_transaction_effect(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|-|
    """

    horizon.start_transaction(
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )
    horizon.commit_transaction(2, UUID(int=1), [])
    commit = horizon.complete_commit(3, UUID(int=1))

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0

    if object == "commit":
        assert commit == Commit(UUID(int=1), set())


@mark.parametrize("object", ["bool", "len"])
def test_commit_completion_without_transaction(
    object: str,
    horizon: Horizon,
) -> None:
    """
    -|
    """

    with raises(NoTransactionError):
        horizon.complete_commit(1, UUID(int=1))

    if object == "bool":
        assert not horizon

    if object == "len":
        assert len(horizon) == 0


@mark.parametrize("object", ["len", "prepared_commit", "completed_commit"])
def test_commit_with_transaction_with_effect(
    object: str,
    horizon: Horizon,
) -> None:
    """
    |--|-|
    """

    horizon.start_transaction(
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )

    prepared_commit = horizon.commit_transaction(
        2,
        UUID(int=1),
        [
            MutatedTuple(tuple_("x", tid=UUID(int=0))),
            NewTuple(tuple_("y", tid=UUID(int=0))),
        ],
    )
    completed_commit = horizon.complete_commit(3, prepared_commit.xid)

    if object == "len":
        assert len(horizon) == 0

    if object == "prepared_commit":
        assert prepared_commit == PreparedCommit(
            UUID(int=1),
            {
                MutatedTuple(tuple_("y", tid=UUID(int=0))),
            },
        )

    if object == "completed_commit":
        assert completed_commit == Commit(
            UUID(int=1),
            {MutatedTuple(tuple_("y", tid=UUID(int=0)))},
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
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )

    horizon.start_transaction(
        2, UUID(int=2), IsolationLevel.serializable_read_and_write
    )

    horizon.commit_transaction(3, UUID(int=1), [])
    horizon.complete_commit(4, UUID(int=1))

    if object == "len_after_commit1":
        assert len(horizon) == 1

    horizon.start_transaction(
        5, UUID(int=3), IsolationLevel.serializable_read_and_write
    )

    horizon.commit_transaction(6, UUID(int=2), [])
    horizon.complete_commit(7, UUID(int=2))

    if object == "len_after_commit1":
        assert len(horizon) == 1

    horizon.commit_transaction(8, UUID(int=3), [])
    horizon.complete_commit(9, UUID(int=3))

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
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )
    commit = horizon.commit_transaction(
        2, UUID(int=1), [MutatedTuple(tuple_("a", tid=UUID(int=1)))]
    )
    commit1 = horizon.complete_commit(3, commit.xid)

    horizon.start_transaction(
        4, UUID(int=2), IsolationLevel.serializable_read_and_write
    )
    commit = horizon.commit_transaction(
        5, UUID(int=2), [MutatedTuple(tuple_("b", tid=UUID(int=1)))]
    )
    commit2 = horizon.complete_commit(6, commit.xid)

    if object == "commit1":
        assert commit1 == Commit(
            UUID(int=1), {MutatedTuple(tuple_("a", tid=UUID(int=1)))}
        )

    if object == "commit2":
        assert commit2 == Commit(
            UUID(int=2), {MutatedTuple(tuple_("b", tid=UUID(int=1)))}
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
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )

    horizon.start_transaction(
        2, UUID(int=2), IsolationLevel.serializable_read_and_write
    )

    commit = horizon.commit_transaction(
        3, UUID(int=1), [MutatedTuple(tuple_("a", tid=UUID(int=1)))]
    )
    commit1 = horizon.complete_commit(4, commit.xid)

    conflict = None
    try:
        horizon.commit_transaction(
            5, UUID(int=2), [MutatedTuple(tuple_("b", tid=UUID(int=1)))]
        )
    except ConflictError as error:
        conflict = error

    if object == "commit1":
        assert commit1 == Commit(
            UUID(int=1), {MutatedTuple(tuple_("a", tid=UUID(int=1)))}
        )

    if object == "commit2":
        assert conflict == ConflictError(UUID(int=2), frozenset())


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
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )

    horizon.start_transaction(
        2, UUID(int=2), IsolationLevel.serializable_read_and_write
    )

    commit2 = horizon.commit_transaction(
        3, UUID(int=2), [MutatedTuple(tuple_("b", tid=UUID(int=1)))]
    )
    commit2 = horizon.complete_commit(4, commit2.xid)

    commit1 = None
    try:
        horizon.commit_transaction(
            5, UUID(int=1), [MutatedTuple(tuple_("a", tid=UUID(int=1)))]
        )
    except ConflictError as error:
        commit1 = error

    if object == "commit1":
        assert commit1 == ConflictError(UUID(int=1), frozenset())

    if object == "commit2":
        assert commit2 == Commit(
            UUID(int=2), {MutatedTuple(tuple_("b", tid=UUID(int=1)))}
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
    |---1-|
     |-2-|
      |----1-|

    Begin order: 123
    Commit order: 213

    2 mutates 2
    1 mutates 1
    3 mutates 1
    """

    horizon.start_transaction(
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )
    horizon.start_transaction(
        2, UUID(int=2), IsolationLevel.serializable_read_and_write
    )
    horizon.start_transaction(
        3, UUID(int=3), IsolationLevel.serializable_read_and_write
    )

    commit = horizon.commit_transaction(
        4, UUID(int=2), [MutatedTuple(tuple_(tid=UUID(int=2)))]
    )
    commit2 = horizon.complete_commit(5, commit.xid)

    commit = horizon.commit_transaction(
        6, UUID(int=1), [MutatedTuple(tuple_(tid=UUID(int=1)))]
    )
    commit1 = horizon.complete_commit(7, commit.xid)

    commit3 = None
    try:
        horizon.commit_transaction(
            8, UUID(int=3), [MutatedTuple(tuple_(tid=UUID(int=1)))]
        )
    except ConflictError as error:
        commit3 = error

    if object == "commit2":
        assert commit2 == Commit(
            UUID(int=2), {MutatedTuple(tuple_(tid=UUID(int=2)))}
        )

    if object == "commit1":
        assert commit1 == Commit(
            UUID(int=1), {MutatedTuple(tuple_(tid=UUID(int=1)))}
        )

    if object == "commit3":
        assert commit3 == ConflictError(UUID(int=3), frozenset())


def test_max_len() -> None:
    """
    # |--
    #  |-
        |
    """

    horizon = horizon_(max_len=2, time=0, max_transaction_age=1000)

    horizon.start_transaction(
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )
    assert len(horizon) == 1

    horizon.start_transaction(
        2, UUID(int=2), IsolationLevel.serializable_read_and_write
    )
    assert len(horizon) == 2

    horizon.start_transaction(
        3, UUID(int=3), IsolationLevel.serializable_read_and_write
    )
    assert len(horizon) == 2


def test_max_transaction_age() -> None:
    """
    ##
    |---
     |--
      |-
       |
    """

    horizon = horizon_(max_len=1000, time=0, max_transaction_age=2)

    horizon.start_transaction(
        1, UUID(int=1), IsolationLevel.serializable_read_and_write
    )
    assert len(horizon) == 1

    horizon.start_transaction(
        2, UUID(int=2), IsolationLevel.serializable_read_and_write
    )
    assert len(horizon) == 2

    horizon.start_transaction(
        3, UUID(int=3), IsolationLevel.serializable_read_and_write
    )
    assert len(horizon) == 2

    horizon.start_transaction(
        4, UUID(int=4), IsolationLevel.serializable_read_and_write
    )
    assert len(horizon) == 2


@mark.timeout(1)
def test_no_memory_leak(horizon: Horizon) -> None:
    """
    |---------||
     |-------||
      |-----||
       |---||
        ...
    """

    transaction_counter = 0

    old_transaction_init = SerializableTransaction.__init__

    def new_transaction_init(*args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        old_transaction_init(*args, **kwargs)

        nonlocal transaction_counter
        transaction_counter += 1

    def transaction_del(self: Transaction, *args: Any, **kwargs: Any) -> None:  # noqa: ARG001,ANN401
        nonlocal transaction_counter
        transaction_counter -= 1

    SerializableTransaction.__init__ = new_transaction_init  # type: ignore[method-assign]
    SerializableTransaction.__del__ = transaction_del  # type: ignore[attr-defined]

    time = 0

    for xid_int in range(1, 101):
        time += 1

        horizon.start_transaction(
            time, UUID(int=xid_int), IsolationLevel.serializable_read_and_write
        )

    assert transaction_counter == 100

    for xid in reversed(range(1, 101)):
        time += 1
        commit = horizon.commit_transaction(time, UUID(int=xid), [])

        time += 1
        horizon.complete_commit(time, commit.xid)

    time += 1
    horizon.start_transaction(
        time, UUID(int=200), IsolationLevel.serializable_read_and_write
    )

    SerializableTransaction.__init__ = old_transaction_init  # type: ignore[method-assign]
    del SerializableTransaction.__del__  # type: ignore[attr-defined]

    assert transaction_counter == 1
