from abc import ABC, abstractmethod
from collections.abc import Sequence

from tgdb.entities.transaction import TransactionEffect


class Heap(ABC):
    @abstractmethod
    async def rows(self, schema: str, )

    @abstractmethod
    async def map(self, effects: Sequence[TransactionEffect], /) -> None: ...

    @abstractmethod
    async def map_as_duplicate(
        self, effects: Sequence[TransactionEffect], /
    ) -> None: ...
