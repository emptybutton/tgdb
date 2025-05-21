from collections.abc import Sequence

from pydantic import BaseModel

from tgdb.entities.transaction import TransactionCommit


class TransactionCommitListSchema(BaseModel):
    commits: Sequence[TransactionCommit]
