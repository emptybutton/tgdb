from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class UvicornConf(BaseModel):
    host: str
    port: int


class APIConf(BaseModel):
    id: int
    hash: str


class ClientsConf(BaseModel):
    bots: Path
    userbots: Path


class TransactionConf(BaseModel):
    max_age_seconds: int = Field(..., alias="max_age_seconds")


class HorizonConf(BaseModel):
    max_len: int
    transaction: TransactionConf


class MessageCacheConf(BaseModel):
    max_len: int


class PageConf(BaseModel):
    fullness: float


class HeapConf(BaseModel):
    chat: int
    page: PageConf


class RelationsConf(BaseModel):
    chat: int


class OverflowConf(BaseModel):
    len: int
    timeout_seconds: int | float


class BufferConf(BaseModel):
    chat: int
    overflow: OverflowConf


class Conf(BaseModel):
    uvicorn: UvicornConf
    api: APIConf
    clients: ClientsConf
    horizon: HorizonConf
    message_cache: MessageCacheConf
    heap: HeapConf
    relations: RelationsConf
    buffer: BufferConf

    @classmethod
    def load(cls, path: Path) -> "Conf":
        with path.open() as file:
            data = yaml.safe_load(file)

        conf = data["conf"]
        return Conf(**conf)
