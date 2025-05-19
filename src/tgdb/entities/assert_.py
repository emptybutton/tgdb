def not_none[ValueT](value: ValueT | None) -> ValueT:
    assert value is not None
    return value
