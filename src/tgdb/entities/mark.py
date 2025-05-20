from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class Mark:
    id: UUID
    key: str
