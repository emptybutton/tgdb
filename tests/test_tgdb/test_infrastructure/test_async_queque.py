from asyncio import gather, sleep, wait_for
from collections.abc import AsyncIterable, Awaitable
from contextlib import suppress
from functools import partial

from pytest import fixture, mark

from tgdb.infrastructure.async_queque import AsyncQueque


type Queque = AsyncQueque[int]


@fixture
def queque() -> Queque:
    return AsyncQueque()


async def iterate(
    queque: Queque,
    result: list[tuple[int, int]],
    iter_: AsyncIterable[int],
) -> None:
    iteration_number = 0

    async for value in iter_:
        result.append((value, len(queque)))
        await sleep(0)

        iteration_number += 1

        if iteration_number == 3:
            break


async def test_iterations_with_prepushed_values(queque: Queque) -> None:
    queque.push(1)
    queque.push(2)
    queque.push(3)

    result = list[tuple[int, int]]()

    iter1 = aiter(queque)
    iter2 = aiter(queque)
    iter3 = aiter(queque)

    await gather(*map(partial(iterate, queque, result), [iter1, iter2, iter3]))

    assert result == [
        (1, 3),
        (1, 3),
        (1, 2),
        (2, 2),
        (2, 2),
        (2, 1),
        (3, 1),
        (3, 1),
        (3, 0),
    ]


async def test_iterations_with_postpushed_values(queque: Queque) -> None:
    result = list[tuple[int, int]]()

    async def iterate(iter_: AsyncIterable[int]) -> None:
        iteration_number = 0

        async for value in iter_:
            result.append((value, len(queque)))
            await sleep(0)

            iteration_number += 1

            if iteration_number == 3:
                break

    async def push() -> None:  # noqa: RUF029
        queque.push(1)
        queque.push(2)
        queque.push(3)

    iter1 = aiter(queque)
    iter2 = aiter(queque)
    iter3 = aiter(queque)

    coros = list[Awaitable[object]]()
    coros.extend(map(iterate, [iter1, iter2, iter3]))
    coros.append(push())

    await gather(*coros)

    assert result == [
        (1, 3),
        (1, 3),
        (1, 2),
        (2, 2),
        (2, 2),
        (2, 1),
        (3, 1),
        (3, 1),
        (3, 0),
    ]


@mark.parametrize("object_", ["result", "queque"])
async def test_iterations_with_concurrent_pushes(
    queque: Queque,
    object_: str,
) -> None:
    result = list[int]()

    async def iterate(iter_: AsyncIterable[int]) -> None:
        iteration_number = 0

        async for value in iter_:
            result.append(value)
            await sleep(0)

            iteration_number += 1

            if iteration_number == 3:
                break

    async def push(value: int) -> None:  # noqa: RUF029
        queque.push(value)

    iter1 = aiter(queque)
    iter2 = aiter(queque)
    iter3 = aiter(queque)

    await gather(
        iterate(iter1),
        push(1),
        iterate(iter2),
        push(2),
        iterate(iter3),
        push(3),
    )

    if object_ == "result":
        assert result == [1, 1, 1, 2, 2, 2, 3, 3, 3]


@mark.parametrize("object_", ["result", "queque"])
async def test_infinite_iterations(queque: Queque, object_: str) -> None:
    result = list[int]()

    async def iterate(iter_: AsyncIterable[int]) -> None:
        async for value in iter_:
            result.append(value)
            await sleep(0)

    async def push(value: int) -> None:  # noqa: RUF029
        queque.push(value)

    queque.push(1)

    iter1 = aiter(queque)
    iter2 = aiter(queque)
    iter3 = aiter(queque)

    main = gather(
        iterate(iter1),
        push(2),
        iterate(iter2),
        iterate(iter3),
        push(3),
    )
    with suppress(TimeoutError):
        await wait_for(main, timeout=0.1)

    if object_ == "result":
        assert result == [1, 1, 1, 2, 2, 2, 3, 3, 3]

    if object_ == "queque":
        assert not queque


@mark.parametrize("object_", ["result", "queque"])
async def test_sync(queque: Queque, object_: str) -> None:
    result = list[int]()
    is_sync_overcome = False

    async def iterate(iter_: AsyncIterable[int]) -> None:
        async for value in iter_:
            result.append(value)
            await sleep(0)

    async def push(value: int) -> None:  # noqa: RUF029
        queque.push(value)

    async def assert_(excepted_result: list[int]) -> None:
        await queque.sync()

        if object_ == "result":
            assert result == excepted_result

        if object_ == "queque":
            assert not queque

        nonlocal is_sync_overcome
        is_sync_overcome = True

    queque.push(1)

    iter1 = aiter(queque)
    iter2 = aiter(queque)
    iter3 = aiter(queque)

    main = gather(
        assert_([1, 1, 1, 2, 2, 2, 3, 3, 3]),
        iterate(iter1),
        push(2),
        iterate(iter2),
        iterate(iter3),
        push(3),
    )
    with suppress(TimeoutError):
        await wait_for(main, timeout=0.1)

    assert is_sync_overcome


@mark.parametrize("object_", ["result", "queque", "iterations_after_sync"])
async def test_deadlock_on_sync(queque: Queque, object_: str) -> None:
    result = list[int]()
    iterations_after_sync = 0

    async def iterate(iter_: AsyncIterable[int]) -> None:
        nonlocal iterations_after_sync

        async for value in iter_:
            result.append(value)

            await queque.sync()
            iterations_after_sync += 1

    queque.push(1)

    main = gather(
        iterate(aiter(queque)),
        iterate(aiter(queque)),
        iterate(aiter(queque)),
    )
    with suppress(TimeoutError):
        await wait_for(main, timeout=0.1)

    if object_ == "result":
        assert result == [1, 1, 1]

    if object_ == "queque":
        assert not queque

    if object_ == "iterations_after_sync":
        assert iterations_after_sync == 0
