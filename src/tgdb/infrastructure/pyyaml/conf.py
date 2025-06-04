from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Conf:
    api_id: int
    api_hash: str

    clients_bots: str
    clients_userbots: str

    heap_chat: int
    heap_page_fullness: float

    relations_chat: int
    relations_vacuum_window_delay_seconds: int
    relations_vacuum_min_message_count: int

    buffer_chat: int
    buffer_overflow_len: int
    buffer_overflow_timeout_seconds: float
    buffer_vacuum_window_delay_seconds: int
    buffer_vacuum_min_message_count: int

    @classmethod
    def load(cls, path: Path) -> "Conf":
        with path.open("r") as file:
            data = yaml.safe_load(file)

        conf = data["conf"]

        return cls(
            api_id=conf["api"]["id"],
            api_hash=conf["api"]["hash"],

            clients_bots=conf["clients"]["bots"],
            clients_userbots=conf["clients"]["userbots"],

            heap_chat=conf["heap"]["chat"],
            heap_page_fullness=conf["heap"]["page"]["fullness"],

            relations_chat=conf["relations"]["chat"],
            relations_vacuum_window_delay_seconds=conf["relations"]["vacuum"]["window_delay_seconds"],
            relations_vacuum_min_message_count=conf["relations"]["vacuum"]["min_message_count"],

            buffer_chat=conf["buffer"]["chat"],
            buffer_overflow_len=conf["buffer"]["overflow"]["len"],
            buffer_overflow_timeout_seconds=conf["buffer"]["overflow"]["timeout_seconds"],
            buffer_vacuum_window_delay_seconds=conf["buffer"]["vacuum"]["window_delay_seconds"],
            buffer_vacuum_min_message_count=conf["buffer"]["vacuum"]["min_message_count"],
        )
