from abc import ABC, abstractmethod
from collections.abc import Sequence

from tgdb.entities.horizon.effect import (
    Claim,
    DeletedTuple,
    MutatedTuple,
    NewTuple,
)


type EncodableEffect = Sequence[NewTuple | MutatedTuple | DeletedTuple | Claim]


class EffectEncoding[EncodedEffectT](ABC):
    @abstractmethod
    async def decoded(
        self, effect: EncodedEffectT, /
    ) -> EncodableEffect | None: ...
