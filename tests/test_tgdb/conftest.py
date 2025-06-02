from pytest import Item, mark
from pytest_asyncio import is_async_test


def pytest_collection_modifyitems(items: list[Item]) -> None:
    async_tests = filter(is_async_test, items)

    markers = (
        mark.timeout(0.75),
        mark.asyncio(loop_scope="session"),
    )

    for async_test in async_tests:
        for marker in reversed(markers):
            async_test.add_marker(marker, append=False)
