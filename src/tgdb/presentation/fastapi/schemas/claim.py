from uuid import UUID

from tgdb.entities.horizon.claim import Claim
from tgdb.presentation.fastapi.schemas.encoding import EncodingSchema


class ClaimSchema(EncodingSchema[Claim]):
    id: UUID
    object: str

    def decoded(self) -> Claim:
        return Claim(self.id, self.object)

    @classmethod
    def of(cls, claim: Claim) -> "ClaimSchema":
        return ClaimSchema(id=claim.id, object=claim.object)
