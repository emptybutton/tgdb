from collections import deque
from collections.abc import AsyncIterator, Sequence
from typing import NewType

from dishka import AnyOf, Provider, Scope, make_container, provide
from in_memory_db import InMemoryDb

from tgdb.application.common.ports.buffer import Buffer
from tgdb.application.common.ports.clock import Clock
from tgdb.application.common.ports.queque import Queque
from tgdb.application.common.ports.uuids import UUIDs
from tgdb.application.horizon.commit_transaction import CommitTransaction
from tgdb.application.horizon.output_commits import OutputCommits
from tgdb.application.horizon.output_commits_to_tuples import (
    OutputCommitsToTuples,
)
from tgdb.application.horizon.ports.channel import Channel
from tgdb.application.horizon.ports.shared_horizon import SharedHorizon
from tgdb.application.horizon.rollback_transaction import RollbackTransaction
from tgdb.application.horizon.start_transaction import StartTransaction
from tgdb.application.relation.create_relation import CreateRelation
from tgdb.application.relation.ports.relations import Relations
from tgdb.application.relation.ports.tuples import Tuples
from tgdb.application.relation.view_tuples import ViewTuples
from tgdb.entities.horizon.horizon import Horizon, horizon
from tgdb.entities.horizon.transaction import Commit, PreparedCommit
from tgdb.entities.relation.relation import Relation
from tgdb.infrastructure.adapters.buffer import (
    InMemoryBuffer,
    InTelegramReplicablePreparedCommitBuffer,
)
from tgdb.infrastructure.adapters.channel import AsyncMapChannel
from tgdb.infrastructure.adapters.clock import PerfCounterClock
from tgdb.infrastructure.adapters.queque import InMemoryQueque
from tgdb.infrastructure.adapters.relations import InTelegramReplicableRelations
from tgdb.infrastructure.adapters.shared_horizon import InMemorySharedHorizon
from tgdb.infrastructure.adapters.tuples import InTelegramHeapTuples
from tgdb.infrastructure.adapters.uuids import UUIDs4
from tgdb.infrastructure.async_map import AsyncMap
from tgdb.infrastructure.async_queque import AsyncQueque
from tgdb.infrastructure.pyyaml.config import TgdbConfig
from tgdb.infrastructure.telethon.client_pool import (
    TelegramClientPool,
    loaded_client_pool_from_farm_file,
)
from tgdb.infrastructure.telethon.in_telegram_bytes import InTelegramBytes
from tgdb.infrastructure.telethon.in_telegram_heap import InTelegramHeap
from tgdb.infrastructure.telethon.lazy_map import (
    MessageIndexLazyMap,
    message_index_lazy_map,
)
from tgdb.infrastructure.typenv.envs import Envs


BotPool = NewType("BotPool", TelegramClientPool)
UserBotPool = NewType("UserBotPool", TelegramClientPool)

RelationCache = NewType("RelationCache", InMemoryDb[Relation])


class MainIOProvider(Provider):
    provide_envs = provide(Envs.load, scope=Scope.APP)

    @provide(scope=Scope.APP)
    def provide_conf(self, envs: Envs) -> TgdbConfig:
        return TgdbConfig.load(envs.config_path)


class CommonProvider(Provider):
    provide_clock = provide(PerfCounterClock, provides=Clock, scope=Scope.APP)
    provide_uuids = provide(UUIDs4, provides=UUIDs, scope=Scope.APP)
    provide_commit_queque = provide(
        staticmethod(lambda: InMemoryQueque(AsyncQueque())),
        provides=Queque[Sequence[Commit | PreparedCommit]],
        scope=Scope.APP,
    )

    @provide(scope=Scope.APP)
    def provide_channel(self, config: TgdbConfig) -> Channel:
        return AsyncMapChannel(
            AsyncMap(),
            config.horizon.transaction.max_age_seconds,
        )

    @provide(scope=Scope.APP)
    async def provide_bot_pool(
        self,
        config: TgdbConfig,
    ) -> AsyncIterator[BotPool]:
        pool = BotPool(
            loaded_client_pool_from_farm_file(
                config.clients.bots,
                config.api.id,
                config.api.hash,
            ),
        )
        async with pool:
            yield pool

    @provide(scope=Scope.APP)
    async def provide_userbot_pool(
        self,
        config: TgdbConfig,
    ) -> AsyncIterator[UserBotPool]:
        pool = UserBotPool(
            loaded_client_pool_from_farm_file(
                config.clients.userbots,
                config.api.id,
                config.api.hash,
            ),
        )
        async with pool:
            yield pool

    @provide(scope=Scope.APP)
    def provide_horizon(self, config: TgdbConfig) -> Horizon:
        return horizon(
            0,
            config.horizon.max_len,
            int(config.horizon.transaction.max_age_seconds * 1_000_000_000),
        )

    @provide(scope=Scope.APP)
    def provide_shared_horizon(self, horizon: Horizon) -> SharedHorizon:
        return InMemorySharedHorizon(horizon)

    @provide(scope=Scope.APP)
    def provide_lazy_message_map(
        self,
        user_bot_pool: UserBotPool,
        config: TgdbConfig,
    ) -> MessageIndexLazyMap:
        return message_index_lazy_map(
            user_bot_pool,
            config.message_cache.max_len,
        )

    @provide(scope=Scope.APP)
    def provide_in_telegram_heap(
        self,
        bot_pool: BotPool,
        user_bot_pool: UserBotPool,
        config: TgdbConfig,
        message_index_lazy_map: MessageIndexLazyMap,
    ) -> InTelegramHeap:
        return InTelegramHeap(
            bot_pool,
            user_bot_pool,
            bot_pool,
            bot_pool,
            config.heap.chat,
            InTelegramHeap.encoded_tuple_max_len(config.heap.page.max_fullness),
            message_index_lazy_map,
        )

    provide_tuples = provide(
        InTelegramHeapTuples,
        provides=Tuples,
        scope=Scope.APP,
    )

    @provide(scope=Scope.APP)
    def provide_in_memory_buffer[ValueT](
        self,
        config: TgdbConfig,
    ) -> InMemoryBuffer[ValueT]:
        return InMemoryBuffer(
            config.buffer.overflow.len,
            config.buffer.overflow.timeout_seconds,
            deque(),
        )

    @provide(scope=Scope.APP)
    async def provide_buffer(
        self,
        config: TgdbConfig,
        bot_pool: BotPool,
        user_bot_pool: UserBotPool,
        in_memory_buffer: InMemoryBuffer[Commit | PreparedCommit],
    ) -> AsyncIterator[Buffer[Commit | PreparedCommit]]:
        in_tg_bytes = InTelegramBytes(
            bot_pool,
            user_bot_pool,
            config.buffer.chat,
        )

        buffer = InTelegramReplicablePreparedCommitBuffer(
            in_memory_buffer,
            in_tg_bytes,
        )

        async with buffer:
            yield buffer

    @provide(
        scope=Scope.APP,
    )
    async def provide_relations(
        self,
        config: TgdbConfig,
        bot_pool: BotPool,
        user_bot_pool: UserBotPool,
    ) -> AsyncIterator[AnyOf[Relations, InTelegramReplicableRelations]]:
        in_tg_bytes = InTelegramBytes(
            bot_pool,
            user_bot_pool,
            config.relations.chat,
        )
        relations = InTelegramReplicableRelations(in_tg_bytes, InMemoryDb())

        async with relations:
            yield relations

    provide_commit_transaction = provide(CommitTransaction, scope=Scope.APP)
    provide_output_commits = provide(OutputCommits, scope=Scope.APP)
    provide_output_commits_to_tuples = provide(
        OutputCommitsToTuples,
        scope=Scope.APP,
    )
    provide_rollback_transaction = provide(RollbackTransaction, scope=Scope.APP)
    provide_start_transaction = provide(StartTransaction, scope=Scope.APP)

    provide_create_relations = provide(CreateRelation, scope=Scope.APP)

    provide_view_tuples = provide(ViewTuples, scope=Scope.APP)


main_io_container = make_container(MainIOProvider())
