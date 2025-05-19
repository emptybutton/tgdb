from abc import ABC, abstractmethod

from tgdb.entities.operator import Operator


class OperatorSerialization[SerializedOperatorT](ABC):
    @abstractmethod
    async def deserialized(
        self, operator: SerializedOperatorT, /
    ) -> Operator | None: ...
