from abc import ABC, abstractmethod
from collections.abc import Sequence

from tgdb.entities.transaction import TransactionEffect


class Heap(ABC):
    @abstractmethod
    async def tuples_with(self, id: TupleID)

    @abstractmethod
    async def map(self, effects: Sequence[TransactionEffect], /) -> None: ...

    @abstractmethod
    async def map_as_dublicates(
        self, effects: Sequence[TransactionEffect], /
    ) -> None: ...
