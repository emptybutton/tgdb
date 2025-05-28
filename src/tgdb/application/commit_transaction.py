from asyncio import gather
from dataclasses import dataclass

from tgdb.application.ports.buffer import Buffer
from tgdb.application.ports.channel import Channel
from tgdb.application.ports.clock import Clock
from tgdb.application.ports.operator_encoding import (
    DeletedTupleOperator,
    MutatedTupleOperator,
    NewTupleOperator,
    Operator,
    OperatorEncoding,
)
from tgdb.application.ports.relations import Relations
from tgdb.application.ports.shared_horizon import SharedHorizon
from tgdb.application.ports.uuids import UUIDs
from tgdb.entities.horizon.claim import Claim
from tgdb.entities.horizon.transaction import XID, Commit, PreparedCommit
from tgdb.entities.relation.tuple_effect import (
    DeletedTuple,
    InvalidTuple,
    MutatedTuple,
    NewTuple,
    deleted_tuple,
    mutated_tuple,
    new_tuple,
)


@dataclass(frozen=True)
class CommitTransaction[EncodedOperatorsT]:
    uuids: UUIDs
    shared_horizon: SharedHorizon
    clock: Clock
    operator_encoding: OperatorEncoding[EncodedOperatorsT]
    relations: Relations
    channel: Channel
    commit_buffer: Buffer[PreparedCommit]

    async def __call__(
        self, xid: XID, encoded_operators: EncodedOperatorsT
    ) -> None:
        """
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.InvalidTransactionStateError:
        :raises tgdb.entities.horizon.horizon.InvalidTupleError:
        :raises tgdb.entities.horizon.horizon.InvalidEffectsError:
        :raises tgdb.entities.horizon.transaction.ConflictError:
        :raises tgdb.entities.horizon.transaction.NonSerializableWriteTransactionError:
        """  # noqa: E501

        operators = await self.operator_encoding.decoded(encoded_operators)

        if operators is None:
            effects = None
        else:
            effects = await gather(*map(self._effect, operators))

        time = await self.clock

        async with self.shared_horizon as horizon:
            commit = horizon.commit_transaction(time, xid, effects)

        if not isinstance(commit, PreparedCommit):
            return

        notification, _ = await gather(
            self.channel.wait(commit.xid),
            self.commit_buffer.add(commit),
        )
        if notification.error is not None:
            raise notification.error from notification.error

    async def _effect(
        self, operator: Operator | None
    ) -> NewTuple | MutatedTuple | DeletedTuple | Claim | InvalidTuple:
        match operator:
            case Claim():
                return operator
            case DeletedTupleOperator():
                return deleted_tuple(operator.tid)
            case None:
                return InvalidTuple(None, None, None)
            case _:
                ...

        if operator.relation_number is None:
            relation = None
        else:
            relation = await self.relations.relation(operator.relation_number)

        match operator:
            case NewTupleOperator():
                tid = await self.uuids.random_uuid()
                return new_tuple(tid, operator.scalars, relation)
            case MutatedTupleOperator():
                return mutated_tuple(operator.tid, operator.scalars, relation)
