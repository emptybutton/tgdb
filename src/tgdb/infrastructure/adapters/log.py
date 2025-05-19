from asyncio import gather
from collections.abc import Sequence
from dataclasses import dataclass
from typing import override

from tgdb.application.ports.log import Log
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator, Operator
from tgdb.infrastructure.logic_clock import LogicClock
from tgdb.infrastructure.map_queuqe import MapQueuqe


@dataclass
class InMemoryLog(Log):
    _queque: MapQueuqe[Operator]
    _clock: LogicClock

    async def push_many(
        self, operators: Sequence[Operator]
    ) -> Sequence[AppliedOperator]:
        return await gather(
            *(self.push_one(operator) for operator in operators)
        )

    async def push_one(self, operator: Operator) -> AppliedOperator:
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
