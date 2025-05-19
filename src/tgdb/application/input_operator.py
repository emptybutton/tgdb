from dataclasses import dataclass

from tgdb.application.ports.operator_serialization import OperatorSerialization
from tgdb.application.ports.queque import Queque
from tgdb.entities.operator import Operator


class InputOperatorError(Exception): ...


@dataclass(frozen=True)
class InputOperator[SerializedOperatorsT]:
    input_operators: Queque[Operator]
    operator_serialization: OperatorSerialization[SerializedOperatorsT]

    async def __call__(
        self, serialized_operator: SerializedOperatorsT
    ) -> None:
        """
        :raises tgdb.application.input_operator.InputOperatorError:
        """

        input_operator = await self.operator_serialization.deserialized(
            serialized_operator
        )

        if input_operator is None:
            raise InputOperatorError

        await self.input_operators.push(input_operator)
