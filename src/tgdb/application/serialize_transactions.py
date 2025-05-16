from dataclasses import dataclass

from tgdb.application.ports.heap import Heap
from tgdb.application.ports.log import LogSlot
from tgdb.application.ports.queque import Queque
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.entities.transaction import TransactionCommit
from tgdb.entities.transaction_horizon import (
    TransactionHorizon,
    create_transaction_horizon,
)


@dataclass(frozen=True)
class SerializeTransactions:
    log_slot: LogSlot
    heap: Heap
    input_operators: Queque[AppliedOperator]
    output_commits: Queque[TransactionCommit]

    async def __call__(self) -> None:
        horizon = create_transaction_horizon()

        async_input_operators = await self.input_operators.async_()

        async for operator in self.log_slot(block=False):
            await self._output_operator(operator, horizon)

        async for operator in async_input_operators:
            await self.log_slot.push(operator)
            await self._output_operator(operator, horizon)

    async def _output_operator(
        self, operator: AppliedOperator, horizon: TransactionHorizon
    ) -> None:
        transaction_commit = horizon.add(operator)

        offset_to_commit = self._safe_offset_to_commit(operator, horizon)
        need_to_commit_offset = offset_to_commit != await self.log_slot.offset()

        if transaction_commit and need_to_commit_offset:
            await self.output_commits.sync_push(transaction_commit)
            await self.log_slot.commit(operator.time)

        elif transaction_commit and not need_to_commit_offset:
            await self.output_commits.async_push(transaction_commit)

        elif not transaction_commit and need_to_commit_offset:
            await self.log_slot.commit(operator.time)

    def _safe_offset_to_commit(
        self, operator: AppliedOperator, horizon: TransactionHorizon
    ) -> LogicTime:
        horizon_beginning = horizon.beginning()

        if horizon_beginning is None:
            return operator.time

        return horizon_beginning - 1
