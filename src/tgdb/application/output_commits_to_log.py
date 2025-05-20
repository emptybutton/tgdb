from dataclasses import dataclass

from tgdb.application.ports.log import Log
from tgdb.application.ports.message import TransactionCommitMessage
from tgdb.application.ports.queque import Queque
from tgdb.entities.operator import CommitOperator, StartOperator
from tgdb.entities.transaction import TransactionOkCommit


@dataclass(frozen=True)
class OutputCommitsToLog:
    log: Log
    output_commit_messages: Queque[TransactionCommitMessage]

    async def __call__(self) -> None:
        async for message in self.output_commit_messages:
            if not isinstance(message.commit, TransactionOkCommit):
                continue

            start_operator = StartOperator(message.commit.transaction_id)
            commit_operator = CommitOperator(
                message.commit.transaction_id, message.commit.effect
            )

            await self.log.push(start_operator)
            await self.log.push(commit_operator)
