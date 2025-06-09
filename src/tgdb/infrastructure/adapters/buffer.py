from asyncio import Event, wait_for
from collections import deque
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from types import TracebackType
from typing import ClassVar, Self

from pydantic import TypeAdapter

from tgdb.application.common.ports.buffer import Buffer
from tgdb.entities.horizon.transaction import Commit, PreparedCommit
from tgdb.infrastructure.pydantic.horizon.commit import (
    EncodableCommit,
    EncodablePreparedCommit,
)
from tgdb.infrastructure.telethon.in_telegram_bytes import InTelegramBytes


@dataclass(frozen=True, unsafe_hash=False)
class InMemoryBuffer[ValueT](Buffer[ValueT]):
    _len_to_overflow: int
    _overflow_timeout_seconds: int | float
    _values: deque[ValueT]
    _is_overflowed: Event = field(init=False, default_factory=Event)

    def __post_init__(self) -> None:
        self._refresh_overflow()

    async def add(self, value: ValueT, /) -> None:
        self._values.append(value)
        self._refresh_overflow()

    async def __aiter__(self) -> AsyncIterator[Sequence[ValueT]]:
        while True:
            try:
                await wait_for(
                    self._is_overflowed.wait(),
                    self._overflow_timeout_seconds,
                )
            except TimeoutError:
                if not self._values:
                    continue

            values = tuple(self._values)
            self._values.clear()

            self._is_overflowed.clear()
            yield values

    def _refresh_overflow(self) -> None:
        if len(self._values) >= self._len_to_overflow:
            self._is_overflowed.set()


@dataclass(frozen=True)
class InTelegramReplicablePreparedCommitBuffer(Buffer[Commit | PreparedCommit]):
    _buffer: Buffer[Commit | PreparedCommit]
    _in_tg_encoded_commits: InTelegramBytes

    _adapter: ClassVar = TypeAdapter(
        tuple[EncodableCommit | EncodablePreparedCommit, ...],
    )

    async def __aenter__(self) -> Self:
        encoded_commits = await self._in_tg_encoded_commits

        if encoded_commits is None:
            return self

        encodable_commits = self._adapter.validate_json(encoded_commits)

        for commit in encodable_commits:
            await self._buffer.add(commit.entity())

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def add(self, commit: Commit | PreparedCommit, /) -> None:
        await self._buffer.add(commit)

    async def __aiter__(
        self,
    ) -> AsyncIterator[Sequence[Commit | PreparedCommit]]:
        async for commits in self._buffer:
            encodable_commits = tuple(map(self._encodable_commit, commits))
            encoded_commits = self._adapter.dump_json(encodable_commits)
            await self._in_tg_encoded_commits.set(encoded_commits)

            yield commits

    def _encodable_commit(
        self,
        commit: Commit | PreparedCommit,
    ) -> EncodableCommit | EncodablePreparedCommit:
        match commit:
            case Commit():
                return EncodableCommit.of(commit)

            case PreparedCommit():
                return EncodablePreparedCommit.of(commit)
