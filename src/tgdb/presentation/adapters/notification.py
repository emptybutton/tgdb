from collections.abc import Sequence
from uuid import UUID

from tgdb.application.ports.notifying import Notifying
from tgdb.entities.transaction import TransactionCommit
from tgdb.presentation.async_map import AsyncMap


class TransactionCommitListNotificationToAsyncMap(
    Notifying[Sequence[TransactionCommit]]
):
    _map: AsyncMap[UUID, TransactionCommit]

    async def send(self, commits: Sequence[TransactionCommit], /) -> None:
        for commit in commits:
            self._map[commit.xid] = commit
