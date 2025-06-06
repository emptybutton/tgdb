from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.common.ports.queque import Queque
from tgdb.application.relation.ports.tuples import Tuples
from tgdb.entities.horizon.transaction import Commit, PreparedCommit


@dataclass(frozen=True)
class OutputCommitsToTuples:
    tuples: Tuples
    output_commits: Queque[Sequence[Commit | PreparedCommit]]

    async def __call__(self) -> None:
        is_previous_map_partial = True

        async for output_commits in self.output_commits:
            effects = tuple(commit.effect for commit in output_commits)

            if is_previous_map_partial:
                await self.tuples.map_idempotently(effects)
                is_previous_map_partial = False
            else:
                await self.tuples.map(effects)
