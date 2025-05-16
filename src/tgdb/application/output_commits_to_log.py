from asyncio import gather
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import chain
from typing import cast

from effect import dead, mutated, new

from tgdb.application.ports.log import LogSlot
from tgdb.application.ports.logic_clock import LogicClock
from tgdb.application.ports.queque import Queque
from tgdb.entities.operator import Operator, OperatorValue, applied_operator
from tgdb.entities.transaction import (
    TransactionCommit,
    TransactionOkCommit,
)
from tgdb.entities.transaction_mark import (
    TransactionState,
    TransactionStateMark,
)


@dataclass(frozen=True)
class OutputCommitsToLog:
    clock: LogicClock
    log: LogSlot
    output_commits: Queque[TransactionCommit]

    async def __call__(self) -> None:
        async for commit in self.output_commits.sync():
            if not isinstance(commit, TransactionOkCommit):
                continue

            operators = tuple(self._operators(commit))
            times = sorted(await gather(*(self.clock for _ in operators)))

            applied_operators = (
                applied_operator(operator, time)
                for operator, time in zip(operators, times, strict=True)
            )

            for applied_operator_ in applied_operators:
                await self.log.push(applied_operator_)

    def _operators(self, commit: TransactionOkCommit) -> Iterable[Operator]:
        row_effects = chain(
            map(new, commit.effect.new_values),
            map(mutated, commit.effect.mutated_values),
            map(dead, commit.effect.dead_values),
        )

        values = (
            *row_effects, TransactionStateMark(TransactionState.committed)
        )
        values = cast(Iterable[OperatorValue], values)

        yield from (
            Operator(value, commit.transaction_id)
            for value in values
        )
