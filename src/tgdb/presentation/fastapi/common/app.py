import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, NewType, Self, cast

from dishka import AsyncContainer
from dishka.integrations.fastapi import setup_dishka
from fastapi import APIRouter, FastAPI

from tgdb.presentation.fastapi.common.error_handling import add_error_handling
from tgdb.presentation.fastapi.common.tags import tags_metadata


FastAPIAppBackground = NewType(
    "FastAPIAppBackground",
    tuple[Callable[[], Coroutine[Any, Any, Any]], ...],
)
FastAPIAppRouters = NewType("FastAPIAppRouters", tuple[APIRouter, ...])
FastAPIAppVersion = NewType("FastAPIAppVersion", str)


@dataclass(frozen=True, unsafe_hash=False)
class LefespanBackground:
    _loop: asyncio.AbstractEventLoop = field(
        default_factory=asyncio.get_running_loop,
    )
    _tasks: set[asyncio.Task[Any]] = field(init=False, default_factory=set)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        for task in self._tasks:
            task.cancel()

    def add(
        self,
        func: Callable[[], Coroutine[Any, Any, Any]],
    ) -> None:
        decorated_func = self._decorator(func)
        self._create_task(decorated_func())

    def _decorator(
        self,
        func: Callable[[], Coroutine[Any, Any, Any]],
    ) -> Callable[[], Coroutine[Any, Any, Any]]:
        async def decorated_func() -> None:
            try:
                await func()
            except Exception as error:
                self._create_task(decorated_func())
                raise error from error

        return decorated_func

    def _create_task(self, coro: Coroutine[Any, Any, Any]) -> None:
        task = self._loop.create_task(coro)
        self._tasks.add(task)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with LefespanBackground() as background:
        for func in cast(FastAPIAppBackground, app.state.background):
            background.add(func)

        yield

        await app.state.dishka_container.close()


async def app_from(container: AsyncContainer) -> FastAPI:
    author_url = "https://github.com/emptybutton"
    repo_url = f"{author_url}/tgdb"
    version: FastAPIAppVersion = await container.get(FastAPIAppVersion)

    app = FastAPI(
        title="tgdb",
        version=version,
        summary="RDBMS over Telegram.",
        openapi_tags=tags_metadata,
        contact={
            "name": "Alexander Smolin",
            "url": author_url,
        },
        license_info={
            "name": "Apache 2.0",
            "url": f"{repo_url}/blob/main/LICENSE",
        },
        lifespan=lifespan,
        root_path=f"/api/{version}",
        docs_url="/",
    )

    app.state.background = await container.get(FastAPIAppBackground)
    routers = await container.get(FastAPIAppRouters)

    for router in routers:
        app.include_router(router)

    setup_dishka(container=container, app=app)
    add_error_handling(app)

    openapi = app.openapi()
    openapi["externalDocs"] = {
        "description": "Github",
        "url": repo_url,
    }

    return app
