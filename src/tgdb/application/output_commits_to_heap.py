from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.ports.heap import Heap
from tgdb.application.ports.queque import Queque
from tgdb.entities.transaction import TransactionCommit, TransactionOkCommit


@dataclass(frozen=True)
class OutputCommitsToHeap:
    heap: Heap
    output_commits: Queque[Sequence[TransactionCommit]]

    async def __call__(self) -> None:
        is_previous_map_partial = True

        async for commits in self.output_commits:
            effects = tuple(
                commit.effect
                for commit in commits
                if isinstance(commit, TransactionOkCommit)
            )

            if is_previous_map_partial:
                await self.heap.map_as_duplicate(effects)
                is_previous_map_partial = False
            else:
                await self.heap.map(effects)
