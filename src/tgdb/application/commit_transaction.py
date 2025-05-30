from asyncio import gather
from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.ports.buffer import Buffer
from tgdb.application.ports.channel import Channel
from tgdb.application.ports.clock import Clock
from tgdb.application.ports.operator import (
    DeletedTupleOperator,
    MutatedTupleOperator,
    NewTupleOperator,
    Operator,
)
from tgdb.application.ports.relations import Relations
from tgdb.application.ports.shared_horizon import SharedHorizon
from tgdb.application.ports.uuids import UUIDs
from tgdb.entities.horizon.claim import Claim
from tgdb.entities.horizon.transaction import XID, Commit, PreparedCommit
from tgdb.entities.relation.tuple_effect import (
    DeletedTuple,
    MutatedTuple,
    NewTuple,
    deleted_tuple,
    mutated_tuple,
    new_tuple,
)


@dataclass(frozen=True)
class CommitTransaction:
    uuids: UUIDs
    shared_horizon: SharedHorizon
    clock: Clock
    relations: Relations
    channel: Channel
    commit_buffer: Buffer[PreparedCommit]

    async def __call__(
        self, xid: XID, operators: Sequence[Operator]
    ) -> None:
        """
        :raises tgdb.application.ports.relations.NoRelationError:
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.InvalidTransactionStateError:
        :raises tgdb.entities.horizon.transaction.ConflictError:
        :raises tgdb.entities.horizon.transaction.NonSerializableWriteTransactionError:
        """  # noqa: E501

        effects = await gather(*map(self._effect, operators))
        time = await self.clock

        async with self.shared_horizon as horizon:
            commit = horizon.commit_transaction(time, xid, effects)

        if isinstance(commit, Commit):
            return

        notification, _ = await gather(
            self.channel.wait(commit.xid),
            self.commit_buffer.add(commit),
        )
        if notification.error is not None:
            raise notification.error from notification.error

    async def _effect(
        self, operator: Operator
    ) -> NewTuple | MutatedTuple | DeletedTuple | Claim:
        """
        :raises tgdb.application.ports.relations.NoRelationError:
        """

        match operator:
            case Claim():
                return operator
            case DeletedTupleOperator():
                return deleted_tuple(operator.tid)
            case _:
                ...

        relation = await self.relations.relation(operator.relation_number)

        match operator:
            case NewTupleOperator():
                tid = await self.uuids.random_uuid()
                return new_tuple(tid, operator.scalars, relation)
            case MutatedTupleOperator():
                return mutated_tuple(operator.tid, operator.scalars, relation)
