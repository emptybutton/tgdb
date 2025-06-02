from asyncio import gather, sleep

from pytest import fixture, mark, raises

from tgdb.infrastructure.async_map import AsyncMap


type Map = AsyncMap[int, str]


@fixture
def map() -> Map:
    return AsyncMap()


@mark.parametrize("object", ["result", "map"])
async def test_get_after_set(map: Map, object: str) -> None:
    map[0] = "x"
    xs = await gather(*(map[0] for _ in range(10)))

    if object == "result":
        assert xs == ["x"] * 10

    if object == "map":
        assert list(map) == [0]


@mark.parametrize("object", ["result", "map"])
async def test_get_before_set(map: Map, object: str) -> None:
    async def set() -> None:
        await sleep(0.01)
        map[0] = "x"

    *xs, _ = await gather(*[*(map[0] for _ in range(10)), set()])

    if object == "result":
        assert xs == ["x"] * 10

    if object == "map":
        assert list(map) == [0]


def test_del_without_key(map: Map) -> None:
    map[0] = "x"

    with raises(KeyError):
        del map[1]


def test_del_with_key(map: Map) -> None:
    map[0] = "x"
    map[1] = "y"

    del map[0]

    assert list(map) == [1]
