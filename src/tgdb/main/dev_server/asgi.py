from tgdb.main.common.asgi import LazyASGIApp
from tgdb.main.server.di import server_container
from tgdb.presentation.fastapi.common.app import app_from


app = LazyASGIApp(lambda: app_from(server_container))
