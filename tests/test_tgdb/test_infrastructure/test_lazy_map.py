from collections import OrderedDict
from itertools import pairwise

from pytest import fixture, mark, raises

from tgdb.infrastructure.lazy_map import LazyMap, NoExternalValue


type Map = LazyMap[int, str]


async def external_value(key: int) -> str | NoExternalValue:  # noqa: RUF029
    return NoExternalValue() if key < 0 else str(key)


@fixture
def map() -> Map:
    return LazyMap(100, external_value)


def test_no_external_value_singleton() -> None:
    no_values = (NoExternalValue() for _ in range(10))

    for first, second in pairwise(no_values):
        assert first is second


def test_without_all(map: Map) -> None:
    cache_map = map.cache_map()

    assert cache_map == OrderedDict()


@mark.parametrize("object", ["result", "map"])
async def test_get_uncached_valid_value(map: Map, object: str) -> None:
    result = await map[1]

    if object == "result":
        assert result == "1"

    if object == "map":
        assert map.cache_map() == OrderedDict({1: "1"})


@mark.parametrize("object", ["result", "map"])
async def test_get_cached_value(map: Map, object: str) -> None:
    map[100] = "X"
    result = await map[100]

    if object == "result":
        assert result == "X"

    if object == "map":
        assert map.cache_map() == OrderedDict({100: "X"})


async def test_get_uncached_invalid_value(map: Map) -> None:
    with raises(KeyError):
        await map[-10]

    assert map.cache_map() == OrderedDict({-10: NoExternalValue()})
