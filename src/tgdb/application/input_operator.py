from dataclasses import dataclass

from tgdb.application.errors.common import (
    InvalidInputOperatorError,
    NotActiveNodeError,
)
from tgdb.application.ports.buffer import Buffer
from tgdb.application.ports.logic_clock import LogicClock
from tgdb.application.ports.operator_serialization import OperatorSerialization
from tgdb.application.ports.shared_horizon import SharedHorizon
from tgdb.entities.operator import AppliedOperator
from tgdb.entities.transaction import TransactionPreparedCommit


@dataclass(frozen=True)
class InputOperator[SerializedOperatorsT]:
    clock: LogicClock
    operator_serialization: OperatorSerialization[SerializedOperatorsT]
    commit_buffer: Buffer[TransactionPreparedCommit]
    shared_horizon: SharedHorizon

    async def __call__(
        self, serialized_operator: SerializedOperatorsT
    ) -> None:
        """
        :raises tgdb.application.errors.common.InvalidInputOperatorError:
        """

        input_operator = await self.operator_serialization.deserialized(
            serialized_operator
        )

        if input_operator is None:
            raise InvalidInputOperatorError

        async with self.shared_horizon as horizon:
            time = await self.clock.time()
            applied_input_operator = AppliedOperator(input_operator, time)

            transaction_commit = horizon.add(applied_input_operator)

            if transaction_commit:
                await self.commit_buffer.add(transaction_commit)
