from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.common.ports.buffer import Buffer
from tgdb.application.common.ports.clock import Clock
from tgdb.application.common.ports.queque import Queque
from tgdb.application.horizon.ports.channel import Channel
from tgdb.application.horizon.ports.shared_horizon import SharedHorizon
from tgdb.entities.horizon.horizon import (
    NoTransactionError,
    TransactionNotCommittingError,
)
from tgdb.entities.horizon.transaction import Commit, PreparedCommit


@dataclass(frozen=True)
class OutputCommits:
    commit_buffer: Buffer[Commit | PreparedCommit]
    channel: Channel
    output_commits: Queque[Sequence[Commit | PreparedCommit]]
    shared_horizon: SharedHorizon
    clock: Clock

    async def __call__(self) -> None:
        async for commits in self.commit_buffer:
            await self.output_commits.push(commits)
            await self.output_commits.sync()

            async with self.shared_horizon as horizon:
                for commit in commits:
                    if isinstance(commit, Commit):
                        await self.channel.publish(commit.xid, None)
                        continue

                    time = await self.clock

                    try:
                        horizon.complete_commit(time, commit.xid)
                    except (
                        NoTransactionError,
                        TransactionNotCommittingError,
                    ) as error:
                        await self.channel.publish(commit.xid, error)
                    else:
                        await self.channel.publish(commit.xid, None)
