from asyncio import gather
from dataclasses import dataclass
from typing import override

from tgdb.application.ports.log import Log, LogOffset
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import (
    AppliedOperator,
    CommitOperator,
    Operator,
    RollbackOperator,
    StartOperator,
)
from tgdb.infrastructure.logic_clock import LogicClock
from tgdb.infrastructure.map_queuqe import MapQueuqe
from tgdb.infrastructure.operator_encoding import (
    encoded_commit_intermediate_operators,
    encoded_commit_operator,
    encoded_rollback_operator,
    encoded_start_operator,
)
from tgdb.infrastructure.telethon.client_pool import TelegramClientPool
from tgdb.infrastructure.telethon.vacuum import Vacuum


@dataclass
class InMemoryLog(Log):
    _queque: MapQueuqe[Operator]
    _clock: LogicClock

    async def push(self, operator: Operator) -> AppliedOperator:
        applied_operator = AppliedOperator(operator, self._clock.time())
        self._queque.push(applied_operator.time, applied_operator.operator)

        return applied_operator

    @override
    async def truncate(self, offset_to_truncate: LogicTime, /) -> None:
        for index, offset in enumerate(self._queque):
            if offset == offset_to_truncate:
                self._queque.pull(count=index + 1)
                break

            if offset > offset_to_truncate:
                self._queque.pull(count=index)
                break


@dataclass(frozen=True)
class TelethonLog(Log):
    chat_id: int
    pool_to_insert: TelegramClientPool
    vacuum: Vacuum

    async def truncate(self, offset: LogOffset) -> None:
        await self.vacuum(self.chat_id, None, offset)

    async def push(self, operator: Operator) -> AppliedOperator:
        match operator:
            case StartOperator():
                return await self._push_start_operator(operator)
            case RollbackOperator():
                return await self._push_rollback_operator(operator)
            case CommitOperator():
                return await self._push_commit_operator(operator)

    async def _push_start_operator(
        self, operator: StartOperator
    ) -> AppliedOperator:
        text = encoded_start_operator(operator)
        message = await self.pool_to_insert().send_message(self.chat_id, text)

        return AppliedOperator(operator, message.id)

    async def _push_rollback_operator(
        self, operator: RollbackOperator
    ) -> AppliedOperator:
        text = encoded_rollback_operator(operator)
        message = await self.pool_to_insert().send_message(self.chat_id, text)

        return AppliedOperator(operator, message.id)

    async def _push_commit_operator(
        self, operator: CommitOperator
    ) -> AppliedOperator:
        encoded_intermediate_operators = encoded_commit_intermediate_operators(
            operator
        )
        encoded_commit_operator_ = encoded_commit_operator(operator)

        await gather(*(
            self.pool_to_insert().send_message(self.chat_id, text)
            for text in encoded_intermediate_operators
        ))

        commit_message = await self.pool_to_insert().send_message(
            self.chat_id, encoded_commit_operator_
        )

        return AppliedOperator(operator, commit_message.id)
