from typing import Literal

from pydantic import BaseModel

from tgdb.entities.horizon.transaction import XID, Commit, PreparedCommit
from tgdb.infrastructure.pydantic.horizon.transaction_effect import (
    EncodableTransactionScalarEffect,
    encodable_transaction_scalar_effect,
)


class EncodableCommit(BaseModel):
    type: Literal["commit"] = "commit"
    xid: XID
    effect: tuple[EncodableTransactionScalarEffect, ...]

    def entity(self) -> Commit:
        return Commit(self.xid, frozenset(it.entity() for it in self.effect))

    @classmethod
    def of(cls, entity: Commit) -> "EncodableCommit":
        return EncodableCommit(
            xid=entity.xid,
            effect=tuple(
                map(encodable_transaction_scalar_effect, entity.effect),
            ),
        )


class EncodablePreparedCommit(BaseModel):
    type: Literal["prepared_commit"] = "prepared_commit"
    xid: XID
    effect: tuple[EncodableTransactionScalarEffect, ...]

    def entity(self) -> Commit:
        return Commit(self.xid, frozenset(it.entity() for it in self.effect))

    @classmethod
    def of(cls, entity: PreparedCommit) -> "EncodablePreparedCommit":
        return EncodablePreparedCommit(
            xid=entity.xid,
            effect=tuple(
                map(encodable_transaction_scalar_effect, entity.effect),
            ),
        )
