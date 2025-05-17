from collections.abc import AsyncIterable
from dataclasses import dataclass

from tgdb.application.ports.commit_serialization import CommitSerialization
from tgdb.application.ports.sync_queque import SyncQueque
from tgdb.entities.transaction import TransactionCommit


class InvalidOperatorError(Exception): ...


@dataclass(frozen=True)
class OutputCommits[SerializedCommitT]:
    output_commits: SyncQueque[TransactionCommit]
    commit_serialization: CommitSerialization[SerializedCommitT]

    async def __call__(self) -> AsyncIterable[SerializedCommitT]:
        async for commit in self.output_commits:
            yield await self.commit_serialization.serialized(commit)
