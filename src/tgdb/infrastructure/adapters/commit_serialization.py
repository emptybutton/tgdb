from dataclasses import dataclass

from tgdb.application.ports.commit_serialization import CommitSerialization
from tgdb.entities.transaction import TransactionCommit


@dataclass(frozen=True)
class CommitSerializationToCommit(CommitSerialization[TransactionCommit]):
    async def serialized(self, commit: TransactionCommit) -> TransactionCommit:
        return commit
