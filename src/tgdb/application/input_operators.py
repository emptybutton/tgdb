from dataclasses import dataclass

from tgdb.application.ports.async_queque import AsyncQueque
from tgdb.application.ports.logic_clock import LogicClock
from tgdb.application.ports.operator_serialization import OperatorSerialization
from tgdb.entities.operator import AppliedOperator, applied_operator


class InputOperatorsError(Exception): ...


@dataclass(frozen=True)
class InputOperators[SerializedOperatorsT]:
    clock: LogicClock
    input_operators: AsyncQueque[AppliedOperator]
    operator_serialization: OperatorSerialization[SerializedOperatorsT]

    async def __call__(
        self, serialized_operators: SerializedOperatorsT
    ) -> None:
        """
        :raises tgdb.application.input_operator.InputOperatorsError:
        """

        input_operators = await self.operator_serialization.deserialized(
            serialized_operators
        )

        if input_operators is None:
            raise InputOperatorsError

        times = await self.clock.times(len(input_operators))

        await self.input_operators.push(*(
            applied_operator(input_operator, time)
            for time, input_operator in zip(times, input_operators, strict=True)
        ))
