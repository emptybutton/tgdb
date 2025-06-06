import uvicorn

from tgdb.infrastructure.pyyaml.config import TgdbConfig
from tgdb.main.common.di import main_io_container


def main() -> None:
    tgdb_config = main_io_container.get(TgdbConfig)

    uvicorn.run(
        "tgdb.main.dev_server.asgi:app",
        host=tgdb_config.uvicorn.host,
        port=tgdb_config.uvicorn.port,
        reload=True,
        reload_dirs=["/app"],
    )


if __name__ == "__main__":
    main()
