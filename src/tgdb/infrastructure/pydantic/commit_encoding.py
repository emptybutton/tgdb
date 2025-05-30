from collections.abc import Sequence

from pydantic import BaseModel

from tgdb.entities.horizon.transaction import PreparedCommit


class PreparedCommitListSchema(BaseModel):
    commits: Sequence[PreparedCommit]
