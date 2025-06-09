from asyncio import gather, sleep

from pytest import fixture, mark, raises

from tgdb.infrastructure.async_map import AsyncMap


type Map = AsyncMap[int, str]


@fixture
def map_() -> Map:
    return AsyncMap()


@mark.parametrize("object_", ["result", "map"])
async def test_get_after_set(map_: Map, object_: str) -> None:
    map_[0] = "x"
    xs = await gather(*(map_[0] for _ in range(10)))

    if object_ == "result":
        assert xs == ["x"] * 10

    if object_ == "map_":
        assert list(map_) == [0]


@mark.parametrize("object_", ["result", "map"])
async def test_get_before_set(map_: Map, object_: str) -> None:
    async def set_() -> None:
        await sleep(0.01)
        map_[0] = "x"

    *xs, _ = await gather(*[*(map_[0] for _ in range(10)), set_()])

    if object_ == "result":
        assert xs == ["x"] * 10

    if object_ == "map":
        assert list(map_) == [0]


def test_del_without_key(map_: Map) -> None:
    map_[0] = "x"

    with raises(KeyError):
        del map_[1]


def test_del_with_key(map_: Map) -> None:
    map_[0] = "x"
    map_[1] = "y"

    del map_[0]

    assert list(map_) == [1]
