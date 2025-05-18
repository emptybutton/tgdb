from dataclasses import dataclass
from typing import override

from tgdb.application.ports.log import Log
from tgdb.entities.logic_time import LogicTime
from tgdb.entities.operator import AppliedOperator
from tgdb.infrastructure.map_queuqe import MapQueuqe


@dataclass
class InMemoryLog(Log):
    _queque: MapQueuqe[LogicTime, AppliedOperator]

    async def push(self, operator: AppliedOperator) -> None:
        self._queque.push(operator.time, operator)

    @override
    async def truncate(self, offset_to_truncate: LogicTime, /) -> None:
        for index, offset in enumerate(self._queque):
            if offset == offset_to_truncate:
                self._queque.pull(count=index + 1)
                break

            if offset > offset_to_truncate:
                self._queque.pull(count=index)
                break
