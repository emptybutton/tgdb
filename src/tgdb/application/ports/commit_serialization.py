from abc import ABC, abstractmethod

from tgdb.entities.transaction import TransactionCommit


class CommitSerialization[SerializedCommitT](ABC):
    @abstractmethod
    async def serialized(
        self, commit: TransactionCommit, /
    ) -> SerializedCommitT: ...
