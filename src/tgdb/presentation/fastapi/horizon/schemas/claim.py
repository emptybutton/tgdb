from uuid import UUID

from pydantic import BaseModel

from tgdb.entities.horizon.claim import Claim


class ClaimSchema(BaseModel):
    id: UUID
    object: str

    def decoded(self) -> Claim:
        return Claim(self.id, self.object)

    @classmethod
    def of(cls, claim: Claim) -> "ClaimSchema":
        return ClaimSchema(id=claim.id, object=claim.object)
