from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager

from dishka import AsyncContainer
from dishka.integrations.fastapi import setup_dishka
from fastapi import APIRouter, FastAPI

from tgdb.presentation.fastapi.tags import tags_metadata


type FastAPIAppRouters = Iterable[APIRouter]
type FastAPIAppVersion = str


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
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
        contact={"name": "Alexander Smolin", "url": author_url},
        license_info={
            "name": "Apache 2.0",
            "url": f"{repo_url}/blob/main/LICENSE",
        },
        lifespan=lifespan,
        root_path=f"/api/{version}",
        docs_url="/",
    )

    routers = await container.get(FastAPIAppRouters)

    for router in routers:
        app.include_router(router)

    setup_dishka(container=container, app=app)

    return app
