from typing import Literal, NoReturn, overload


def not_none[ValueT](
    value: ValueT | None,
    else_: Exception | type[Exception] = ValueError,
) -> ValueT:
    if value is not None:
        return value

    raise else_


@overload
def assert_(
    assertion: Literal[False],
    else_: Exception | type[Exception],
) -> NoReturn: ...


@overload
def assert_(
    assertion: Literal[True],
    else_: Exception | type[Exception],
) -> None: ...


@overload
def assert_(assertion: bool, else_: Exception | type[Exception]) -> None: ...  # noqa: FBT001


def assert_(assertion: bool, else_: Exception | type[Exception]) -> None:  # noqa: FBT001
    if not assertion:
        raise else_
