from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class Mark:
    mark_id: UUID
    key: str
