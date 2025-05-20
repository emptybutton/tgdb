from dataclasses import dataclass

from tgdb.application.ports.heap import Heap
from tgdb.application.ports.message import TransactionCommitMessage
from tgdb.application.ports.queque import Queque
from tgdb.entities.transaction import TransactionOkCommit


@dataclass(frozen=True)
class OutputCommitsToHeap:
    heap: Heap
    output_commit_messages: Queque[TransactionCommitMessage]

    async def __call__(self) -> None:
        async for message in self.output_commit_messages:
            if not isinstance(message.commit, TransactionOkCommit):
                return

            if message.is_commit_duplicate:
                await self.heap.map_as_duplicate(message.commit.effect)
            else:
                await self.heap.map(message.commit.effect)
