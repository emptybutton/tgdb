from dataclasses import dataclass

from tgdb.application.ports.heap import Heap
from tgdb.application.ports.log import Log
from tgdb.application.ports.logic_clock import LogicClock
from tgdb.application.ports.queque import Queque
from tgdb.entities.operator import Operator, applied_operator
from tgdb.entities.transaction import (
    TransactionCommit,
    TransactionFailedCommit,
    TransactionOkCommit,
)
from tgdb.entities.transaction_horizon import (
    TransactionHorizon,
    create_transaction_horizon,
)


@dataclass(frozen=True)
class DynamicallyReplicateLogToHeap:
    clock: LogicClock
    log: Log
    heap: Heap
    input_operators: Queque[Operator]
    output_commits: Queque[TransactionCommit]

    async def __call__(self) -> None:
        horizon = create_transaction_horizon()

        await self._recover_after_crash(horizon)
        await self._accept_input_operators(horizon)

    async def _recover_after_crash(self, horizon: TransactionHorizon) -> None:
        async for operator in self.log(block=False):
            result = horizon.take(operator)

            match result:
                case TransactionOkCommit(_, effect) as commit:
                    await self.heap.map(effect)
                    await self.output_commits.push(commit)

                case TransactionFailedCommit() as commit:
                    await self.output_commits.push(commit)

                case None:
                    ...

            horizon_beginning = horizon.beginning()

            if horizon_beginning is None:
                await self.log.commit(operator.time)
            else:
                await self.log.commit(horizon_beginning - 1)

    async def _accept_input_operators(
        self, horizon: TransactionHorizon
    ) -> None:
        async for operator in self.input_operators:
            current_time = await self.clock

            applied_operator_ = applied_operator(operator, current_time)
            await self.log.push(applied_operator_)

            result = horizon.take(applied_operator_)

            match result:
                case TransactionOkCommit(_, effect) as commit:
                    await self.heap.map(effect)
                    await self.output_commits.push(commit)

                case TransactionFailedCommit() as commit:
                    await self.output_commits.push(commit)

                case None:
                    ...

            horizon_beginning = horizon.beginning()

            if horizon_beginning is None:
                await self.log.commit(applied_operator_.time)
            else:
                await self.log.commit(horizon_beginning - 1)
