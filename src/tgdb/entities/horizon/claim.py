from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class Claim:
    id: UUID
    object: str
