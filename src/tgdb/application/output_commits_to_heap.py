from dataclasses import dataclass

from tgdb.application.ports.heap import Heap
from tgdb.application.ports.queque import Queque
from tgdb.entities.transaction import (
    TransactionCommit,
    TransactionOkCommit,
)


@dataclass(frozen=True)
class OutputCommitsToHeap:
    heap: Heap
    output_commits: Queque[TransactionCommit]

    async def __call__(self) -> None:
        async for commit in self.output_commits.sync():
            if isinstance(commit, TransactionOkCommit):
                await self.heap.map(commit.effect)
