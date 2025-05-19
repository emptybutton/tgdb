from dataclasses import dataclass

from tgdb.application.ports.log import Log
from tgdb.application.ports.logic_clock import LogicClock
from tgdb.application.ports.sync_queque import SyncQueque
from tgdb.entities.operator import Operator, applied_operator
from tgdb.entities.transaction import (
    TransactionCommit,
    TransactionOkCommit,
)


@dataclass(frozen=True)
class OutputCommitsToLog:
    clock: LogicClock
    log: Log
    output_commits: SyncQueque[TransactionCommit]

    async def __call__(self) -> None:
        async for commit in self.output_commits:
            if not isinstance(commit, TransactionOkCommit):
                continue

            operators = (
                Operator(effect, commit.transaction_id)
                for effect in commit.effect
            )

            chronology = await self.clock.chronology(len(commit.effect))

            applied_operators = (
                applied_operator(operator, time)
                for operator, time in zip(operators, chronology, strict=True)
            )

            await self.log.push(*applied_operators)
