from dataclasses import dataclass

from tgdb.application.ports.operator_serialization import OperatorSerialization
from tgdb.entities.operator import Operator


@dataclass(frozen=True)
class OperatorSerializationToOperator(OperatorSerialization[Operator | None]):
    async def deserialized(self, operator: Operator | None) -> Operator | None:
        return operator
