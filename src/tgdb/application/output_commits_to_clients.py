from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.ports.notification import Notification
from tgdb.application.ports.queque import Queque
from tgdb.entities.transaction import TransactionCommit


class InvalidOperatorError(Exception): ...


@dataclass(frozen=True)
class OutputCommitsToClients:
    output_commits: Queque[Sequence[TransactionCommit]]
    notification: Notification[Sequence[TransactionCommit]]

    async def __call__(self) -> None:
        async for commits in self.output_commits:
            await self.notification.send(commits)
