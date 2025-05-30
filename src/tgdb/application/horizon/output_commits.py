from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.common.ports.buffer import Buffer
from tgdb.application.common.ports.channel import Channel
from tgdb.application.common.ports.clock import Clock
from tgdb.application.common.ports.queque import Queque
from tgdb.application.common.ports.shared_horizon import SharedHorizon
from tgdb.entities.horizon.horizon import (
    InvalidTransactionStateError,
    NoTransactionError,
)
from tgdb.entities.horizon.transaction import XID, PreparedCommit


@dataclass(frozen=True)
class OutputCommits:
    commit_buffer: Buffer[PreparedCommit]
    channel: Channel
    output_commits: Queque[Sequence[PreparedCommit]]
    shared_horizon: SharedHorizon
    clock: Clock

    async def __call__(self) -> None:
        async for prepared_commits in self.commit_buffer:
            await self.output_commits.push(prepared_commits)
            await self.output_commits.sync()

            ok_commit_xids = list[XID]()
            error_commit_map = dict[
                XID, NoTransactionError | InvalidTransactionStateError
            ]()

            async with self.shared_horizon as horizon:
                for prepared_commit in prepared_commits:
                    time = await self.clock

                    try:
                        horizon.complete_commit(time, prepared_commit.xid)
                    except (
                        NoTransactionError, InvalidTransactionStateError
                    ) as error:
                        error_commit_map[prepared_commit.xid] = error
                    else:
                        ok_commit_xids.append(prepared_commit.xid)

                await self.channel.publish(ok_commit_xids, error_commit_map)
