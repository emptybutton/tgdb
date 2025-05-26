def not_none[ValueT](
    value: ValueT | None, *, error: Exception | type[Exception] = ValueError
) -> ValueT:
    if value is not None:
        return value

    raise error
