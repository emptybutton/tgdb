from abc import ABC, abstractmethod
from collections.abc import Sequence

from tgdb.entities.horizon.transaction import TransactionEffect
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import Tuple


class Tuples(ABC):
    @abstractmethod
    async def tuples_with_attribute(
        self,
        relation_number: Number,
        attribute_number: Number,
        attribute_scalar: Scalar,
        /,
    ) -> Sequence[Tuple]: ...

    @abstractmethod
    async def map(self, effects: Sequence[TransactionEffect], /) -> None: ...

    @abstractmethod
    async def map_idempotently(
        self, effects: Sequence[TransactionEffect], /
    ) -> None: ...
