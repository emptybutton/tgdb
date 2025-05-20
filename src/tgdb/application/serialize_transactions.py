from dataclasses import dataclass

from tgdb.application.ports.log import Log, LogOffset
from tgdb.application.ports.log_iterator import LogIterator
from tgdb.application.ports.message import TransactionCommitMessage
from tgdb.application.ports.queque import Queque
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator, Operator
from tgdb.entities.transaction import TransactionCommit
from tgdb.entities.transaction_horizon import (
    TransactionHorizon,
    create_transaction_horizon,
)


@dataclass(frozen=True)
class SerializeTransactions:
    log: Log
    log_iterator: LogIterator
    input_operators: Queque[Operator]
    output_commit_messages: Queque[TransactionCommitMessage]

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

        input_operators = aiter(self.input_operators)

        async for operator in self.log_iterator.finite():
            await self._output_operator(operator, horizon, is_duplicate=True)

        async for operator in input_operators:
            applied_operator = await self.log.push(operator)
            await self._output_operator(
                applied_operator, horizon, is_duplicate=False
            )

    async def _output_operator(
        self,
        operator: AppliedOperator,
        horizon: TransactionHorizon,
        is_duplicate: bool,
    ) -> None:
        transaction_commit = horizon.add(operator)

        offset_to_commit = self._safe_offset_to_commit(operator, horizon)
        need_to_commit_offset = (
            offset_to_commit != await self.log_iterator.offset()
        )

        if transaction_commit:
            await self.output_commit_messages.push(
                TransactionCommitMessage(
                    transaction_commit, is_commit_duplicate=is_duplicate
                )
            )

        if need_to_commit_offset:
            await self.output_commit_messages.sync()
            await self.log_iterator.commit(offset_to_commit)

    def _safe_offset_to_commit(
        self, operator: AppliedOperator, horizon: TransactionHorizon
    ) -> LogOffset:
        horizon_beginning = horizon.beginning()

        if horizon_beginning is None:
            return operator.time

        return horizon_beginning - 1
