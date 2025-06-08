from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class UvicornConfig(BaseModel):
    host: str
    port: int


class APIConfig(BaseModel):
    id: int
    hash: str


class ClientsConfig(BaseModel):
    bots: Path
    userbots: Path


class TransactionConfig(BaseModel):
    max_age_seconds: int = Field(..., alias="max_age_seconds")


class HorizonConfig(BaseModel):
    max_len: int
    transaction: TransactionConfig


class MessageCacheConfig(BaseModel):
    max_len: int


class PageConfig(BaseModel):
    max_fullness: float


class HeapConfig(BaseModel):
    chat: int
    page: PageConfig


class RelationsConfig(BaseModel):
    chat: int


class OverflowConfig(BaseModel):
    len: int
    timeout_seconds: int | float


class BufferConfig(BaseModel):
    chat: int
    overflow: OverflowConfig


class TgdbConfig(BaseModel):
    uvicorn: UvicornConfig
    api: APIConfig
    clients: ClientsConfig
    horizon: HorizonConfig
    message_cache: MessageCacheConfig
    heap: HeapConfig
    relations: RelationsConfig
    buffer: BufferConfig

    @classmethod
    def load(cls, path: Path) -> "TgdbConfig":
        with path.open() as file:
            data = yaml.safe_load(file)

        conf = data["conf"]
        return TgdbConfig(**conf)
