from collections.abc import Awaitable, Callable, MutableMapping
from dataclasses import dataclass, field
from typing import Any


Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]

Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


@dataclass
class LazyASGIApp:
    _app: Callable[[], Awaitable[ASGIApp]]
    _cached_app: ASGIApp | None = field(default=None, init=False)

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if self._cached_app is None:
            self._cached_app = await self._app()

        await self._cached_app(scope, receive, send)
