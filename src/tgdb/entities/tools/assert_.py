from typing import Literal, NoReturn, overload


def not_none[ValueT](
    value: ValueT | None, *, error: Exception | type[Exception] = ValueError
) -> ValueT:
    if value is not None:
        return value

    raise error


@overload
def assert_(
    assertion: Literal[False], else_: Exception | type[Exception]
) -> NoReturn: ...


@overload
def assert_(
    assertion: Literal[True], else_: Exception | type[Exception]
) -> None: ...


@overload
def assert_(assertion: bool, else_: Exception | type[Exception]) -> None: ...


def assert_(assertion: bool, else_: Exception | type[Exception]) -> None:
    if not assertion:
        raise else_
