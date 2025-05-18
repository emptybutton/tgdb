from abc import ABC, abstractmethod
from collections.abc import Sequence

from tgdb.entities.operator import Operator


class OperatorSerialization[SerializedOperatorsT](ABC):
    @abstractmethod
    async def deserialized(
        self, operators: SerializedOperatorsT, /
    ) -> Sequence[Operator] | None: ...
