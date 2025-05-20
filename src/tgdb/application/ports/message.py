from dataclasses import dataclass

from tgdb.entities.transaction import TransactionCommit


@dataclass(frozen=True)
class TransactionCommitMessage:
    commit: TransactionCommit
    is_commit_duplicate: bool
