from dataclasses import dataclass

from tgdb.application.ports.log import Log
from tgdb.application.ports.queque import Queque
from tgdb.entities.operator import CommitOperator, StartOperator
from tgdb.entities.transaction import (
    TransactionCommit,
    TransactionOkCommit,
)


@dataclass(frozen=True)
class OutputCommitsToLog:
    log: Log
    output_commits: Queque[TransactionCommit]

    async def __call__(self) -> None:
        async for commit in self.output_commits:
            if not isinstance(commit, TransactionOkCommit):
                continue

            start_operator = StartOperator(commit.transaction_id)
            commit_operator = CommitOperator(
                commit.transaction_id, commit.effect
            )

            await self.log.push_many((start_operator, commit_operator))
