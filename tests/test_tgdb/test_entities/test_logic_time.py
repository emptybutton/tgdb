from pytest import mark

from tgdb.entities.logic_time import LogicTime, age


@mark.parametrize(
    "args, output",
    [
        [(5, 5), 1],
        [(5, 6), 2],
        [(None, 5), 0],
        [(-5, -5), 1],
        [(-5, -4), 2],
    ],
)
def test_age(
    args: tuple[LogicTime | None, LogicTime], output: LogicTime
) -> None:
    assert age(*args) == output
