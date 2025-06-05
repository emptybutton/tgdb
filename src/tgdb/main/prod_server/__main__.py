import asyncio

import uvicorn

from tgdb.infrastructure.pyyaml.conf import Conf
from tgdb.main.server.di import server_container
from tgdb.presentation.fastapi.common.app import app_from


async def amain() -> None:
    app = await app_from(server_container)
    conf = await server_container.get(Conf)

    config = uvicorn.Config(
        app,
        host=conf.uvicorn.host,
        port=conf.uvicorn.port,
    )
    server = uvicorn.Server(config)

    await server.serve()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
