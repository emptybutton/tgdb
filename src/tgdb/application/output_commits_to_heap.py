from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.ports.heap import Heap
from tgdb.application.ports.queque import Queque
from tgdb.entities.transaction import TransactionOkPreparedCommit


@dataclass(frozen=True)
class OutputCommitsToHeap:
    heap: Heap
    output_commits: Queque[Sequence[TransactionOkPreparedCommit]]

    async def __call__(self) -> None:
        async for commits in self.output_commits:
            effects = tuple(commit.effect for commit in commits)

            await self.heap.map(effects)
