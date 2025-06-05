from typing import Any

from telethon.sessions.string import StringSession


class StringSessionWithoutEntites(StringSession):
    def process_entities(self, tlo: Any) -> None:  # noqa: ANN401
        ...
