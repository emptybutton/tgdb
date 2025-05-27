from abc import ABC, abstractmethod

from tgdb.entities.horizon.transaction import Commit, ConflictError, NonSerializableWriteTransactionError


type Publishable = Commit | ConflictError | NonSerializableWriteTransactionError


class Notifying(ABC):
    @abstractmethod
    async def publish(self, value: Publishable, /) -> None: ...
