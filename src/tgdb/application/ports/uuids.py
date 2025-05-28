from abc import ABC, abstractmethod
from uuid import UUID


class UUIDs(ABC):
    @abstractmethod
    async def random_uuid(self) -> UUID: ...
