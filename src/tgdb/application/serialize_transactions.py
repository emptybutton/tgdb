from dataclasses import dataclass

from tgdb.application.ports.buffer import Buffer
from tgdb.application.ports.logic_clock import LogicClock
from tgdb.application.ports.queque import Queque
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator, Operator
from tgdb.entities.transaction import TransactionCommit
from tgdb.entities.transaction_horizon import create_transaction_horizon


@dataclass(frozen=True)
class SerializeTransactions:
    input_operators: Queque[Operator]
    clock: LogicClock
    commit_buffer: Buffer[TransactionCommit]

    async def __call__(
        self,
        horizon_max_width: LogicTime | None,
        horizon_max_height: int | None,
    ) -> None:
        """
        :raises tgdb.entities.transaction_horizon.UnlimitedTransactionHorizonError:
        :raises tgdb.entities.transaction_horizon.UnattainableTransactionHorizonError:
        :raises tgdb.entities.transaction_horizon.UselessMaxHeightError:
        """  # noqa: E501

        horizon = create_transaction_horizon(
            horizon_max_width, horizon_max_height
        )

        async for operator in self.input_operators:
            time = await self.clock.time()
            applied_operator = AppliedOperator(operator, time)

            transaction_commit = horizon.add(applied_operator)

            if transaction_commit:
                await self.commit_buffer.add(transaction_commit)
