from dataclasses import dataclass

from tgdb.application.ports.logic_clock import LogicClock
from tgdb.application.ports.operator_serialization import OperatorSerialization
from tgdb.application.ports.queque import Queque
from tgdb.entities.operator import AppliedOperator, applied_operator


class InvalidInputOperatorError(Exception): ...


@dataclass(frozen=True)
class InputOperator[SerializedOperatorT]:
    clock: LogicClock
    input_operators: Queque[AppliedOperator]
    operator_serialization: OperatorSerialization[SerializedOperatorT]

    async def __call__(self, serialized_operator: SerializedOperatorT) -> None:
        """
        :raises tgdb.application.input_operator.InvalidInputOperatorError:
        """

        input_operator = await self.operator_serialization.deserialized(
            serialized_operator
        )

        if input_operator is None:
            raise InvalidInputOperatorError

        current_time = await self.clock
        applied_operator_ = applied_operator(input_operator, current_time)

        await self.input_operators.async_push(applied_operator_)
