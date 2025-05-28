from abc import ABC, abstractmethod

from tgdb.entities.relation.tuple import Tuple
from tgdb.entities.relation.versioned_tuple import VersionedTuple


class Migrations(ABC):
    @abstractmethod
    async def migrate(self, tuple: Tuple) -> VersionedTuple: ...
