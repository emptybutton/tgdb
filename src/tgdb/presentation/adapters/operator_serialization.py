from dataclasses import dataclass

from tgdb.application.common.ports.operator_serialization import OperatorSerialization
from tgdb.entities.operator import Operator
from tgdb.presentation.fastapi.schemas.entity import EntitySchema


@dataclass(frozen=True)
class OperatorSerializationToOperator(OperatorSerialization[Operator | None]):
    async def deserialized(self, operator: Operator | None) -> Operator | None:
        return operator


@dataclass(frozen=True)
class OperatorSerializationToEntitySchema[OperatorT: Operator](
    OperatorSerialization[EntitySchema[OperatorT]]
):
    async def deserialized(self, schema: EntitySchema[OperatorT]) -> OperatorT:
        return schema.entity()
