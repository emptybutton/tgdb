import asyncio

import uvicorn

from tgdb.infrastructure.pyyaml.config import TgdbConfig
from tgdb.main.server.di import server_container
from tgdb.presentation.fastapi.common.app import app_from


async def amain() -> None:
    app = await app_from(server_container)
    tgdb_config = await server_container.get(TgdbConfig)

    uvicorn_config = uvicorn.Config(
        app,
        host=tgdb_config.uvicorn.host,
        port=tgdb_config.uvicorn.port,
    )
    server = uvicorn.Server(uvicorn_config)

    await server.serve()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
