from dataclasses import dataclass, field
from uuid import UUID, uuid4

from tgdb.application.common.ports.uuids import UUIDs


@dataclass(frozen=True)
class UUIDs4(UUIDs):
    async def random_uuid(self) -> UUID:
        return uuid4()


@dataclass
class MonotonicUUIDs(UUIDs):
    _counter: int = field(init=False, default=0)

    async def random_uuid(self) -> UUID:
        next_uuid = UUID(int=self._counter)
        self._counter += 1

        return next_uuid
