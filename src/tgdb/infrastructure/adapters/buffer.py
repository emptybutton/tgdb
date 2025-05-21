from asyncio import Event, wait_for
from collections import deque
from collections.abc import AsyncIterator, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self

from tgdb.application.ports.buffer import Buffer
from tgdb.entities.transaction import TransactionCommit
from tgdb.infrastructure.pydantic.commit_encoding import (
    TransactionCommitListSchema,
)
from tgdb.infrastructure.telethon.in_telegram_big_text import InTelegramBigText


@dataclass(frozen=True, unsafe_hash=False)
class InMemoryBuffer[ValueT](Buffer[ValueT]):
    _size_to_overflow: int
    _overflow_timeout_seconds: int
    _values: deque[ValueT]
    _is_overflowed: Event = field(init=False, default_factory=Event)

    def __post_init__(self) -> None:
        self._refresh_overflow()

    async def add(self, value: ValueT, /) -> None:
        self._values.append(value)
        self._refresh_overflow()

    async def __aiter__(self) -> AsyncIterator[Sequence[ValueT]]:
        while True:
            with suppress(TimeoutError):
                await wait_for(
                    self._is_overflowed.wait(), self._overflow_timeout_seconds
                )
                self._is_overflowed.clear()

            yield tuple(
                self._values.popleft()
                for _ in range(self._size_to_overflow)
            )

    def _refresh_overflow(self) -> None:
        if len(self._values) >= self._size_to_overflow:
            self._is_overflowed.set()


@dataclass(frozen=True)
class InTelegramReplicableTransactionCommitBuffer(Buffer[TransactionCommit]):
    _buffer: Buffer[TransactionCommit]
    _in_tg_encoded_commits: InTelegramBigText

    async def __aenter__(self) -> Self:
        encoded_commits = await self._in_tg_encoded_commits

        if encoded_commits is None:
            return self

        commit_list_schema = (
            TransactionCommitListSchema.model_validate_json(encoded_commits)
        )
        for commit in commit_list_schema.commits:
            await self._buffer.add(commit)

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def add(self, commit: TransactionCommit, /) -> None:
        await self._buffer.add(commit)

    async def __aiter__(self) -> AsyncIterator[Sequence[TransactionCommit]]:
        async for commits in self._buffer:
            commit_list_schema = TransactionCommitListSchema(commits=commits)
            encoded_encoded_commits = commit_list_schema.model_dump_json()

            await self._in_tg_encoded_commits.set(encoded_encoded_commits)

            yield commits
