from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.ports.buffer import Buffer
from tgdb.application.ports.notifying import Notifying
from tgdb.application.ports.queque import Queque
from tgdb.application.ports.shared_horizon import SharedHorizon
from tgdb.entities.horizon.transaction import PreparedCommit


@dataclass(frozen=True)
class OutputCommits:
    commit_buffer: Buffer[PreparedCommit]
    notifying: Notifying[Sequence[TransactionCommit]]
    output_commits: Queque[Sequence[TransactionCommit]]
    shared_horizon: SharedHorizon

    async def __call__(self) -> None:
        async for prepared_commits in self.commit_buffer:
            await self.notifying.publish(prepared_commits)

            ok_prepared_commits = tuple(
                commit for commit in prepared_commits
                if isinstance(commit, TransactionCommit)
            )

            await self.output_commits.push(ok_prepared_commits)
            await self.output_commits.sync()

            async with self.shared_horizon as horizon:
                for ok_prepared_commit in ok_prepared_commits:
                    horizon.complete(ok_prepared_commit)
