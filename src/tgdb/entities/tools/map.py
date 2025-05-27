from collections.abc import Mapping
from typing import Any


def first_map_value[ValueT](map: Mapping[Any, ValueT]) -> ValueT | None:
    try:
        return next(iter(map.values()))
    except StopIteration:
        return None
