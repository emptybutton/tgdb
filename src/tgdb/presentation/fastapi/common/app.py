import asyncio
from collections.abc import AsyncIterator, Coroutine, Iterable
from contextlib import asynccontextmanager, suppress
from typing import Any, cast

from dishka import AsyncContainer
from dishka.integrations.fastapi import setup_dishka
from fastapi import APIRouter, FastAPI

from tgdb.presentation.fastapi.common.tags import tags_metadata


type FastAPIAppCoroutines = Iterable[Coroutine[Any, Any, Any]]
type FastAPIAppRouters = Iterable[APIRouter]
type FastAPIAppVersion = str


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    with suppress(asyncio.CancelledError):
        async with asyncio.TaskGroup() as tasks:
            for coroutine in cast(FastAPIAppCoroutines, app.state.coroutines):
                tasks.create_task(coroutine)

            yield

            await app.state.dishka_container.close()
            raise asyncio.CancelledError


async def app_from(container: AsyncContainer) -> FastAPI:
    author_url = "https://github.com/emptybutton"
    repo_url = f"{author_url}/tgdb"
    version: FastAPIAppVersion = await container.get(FastAPIAppVersion)

    app = FastAPI(
        title="tgdb",
        version=version,
        summary="RDBMS over Telegram.",
        openapi_tags=tags_metadata,
        contact={"name": "Alexander Smolin", "url": author_url},
        license_info={
            "name": "Apache 2.0",
            "url": f"{repo_url}/blob/main/LICENSE",
        },
        lifespan=lifespan,
        root_path=f"/api/{version}",
        docs_url="/",
    )

    app.state.coroutines = await container.get(FastAPIAppCoroutines)
    routers = await container.get(FastAPIAppRouters)

    for router in routers:
        app.include_router(router)

    setup_dishka(container=container, app=app)

    return app
