from dataclasses import dataclass

from tgdb.application.ports.async_queque import AsyncQueque
from tgdb.application.ports.log import Log, LogOffset
from tgdb.application.ports.log_iterator import LogIterator
from tgdb.application.ports.sync_queque import SyncQueque
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.entities.transaction import TransactionCommit
from tgdb.entities.transaction_horizon import (
    TransactionHorizon,
    create_transaction_horizon,
)


@dataclass(frozen=True)
class SerializeTransactions:
    log: Log
    log_iterator: LogIterator
    input_operators: AsyncQueque[AppliedOperator]
    output_commits: SyncQueque[TransactionCommit]

    async def __call__(self, max_transaction_horizon_age: LogicTime) -> None:
        horizon = create_transaction_horizon(max_transaction_horizon_age)

        input_operator_iter = await self.input_operators.iter()

        async for operator in self.log_iterator.finite():
            await self._output_operator(operator, horizon)

        async for operator in input_operator_iter:
            await self.log.push(operator)
            await self._output_operator(operator, horizon)

    async def _output_operator(
        self, operator: AppliedOperator, horizon: TransactionHorizon
    ) -> None:
        transaction_commit = horizon.add(operator)

        offset_to_commit = self._safe_offset_to_commit(operator, horizon)
        need_to_commit_offset = (
            offset_to_commit != await self.log_iterator.offset()
        )

        if transaction_commit:
            await self.output_commits.push(transaction_commit)

        if need_to_commit_offset:
            await self.output_commits.sync()
            await self.log_iterator.commit(operator.time)

    def _safe_offset_to_commit(
        self, operator: AppliedOperator, horizon: TransactionHorizon
    ) -> LogOffset:
        horizon_beginning = horizon.beginning()

        if horizon_beginning is None:
            return operator.time

        return horizon_beginning - 1
