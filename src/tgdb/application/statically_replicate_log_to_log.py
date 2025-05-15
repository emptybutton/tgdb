from dataclasses import dataclass

from tgdb.application.ports.log import Log


@dataclass(frozen=True)
class StaticallyReplicateLogToLog:
    master_log: Log
    replica_log: Log

    async def __call__(self) -> None:
        async for operator in self.master_log(block=True):
            await self.replica_log.push(operator)
            await self.master_log.commit(operator.time)
