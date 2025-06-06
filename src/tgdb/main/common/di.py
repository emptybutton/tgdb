from collections import deque
from collections.abc import AsyncIterator, Sequence
from typing import NewType

from dishka import Provider, Scope, make_container, provide
from in_memory_db import InMemoryDb

from tgdb.application.common.ports.buffer import Buffer
from tgdb.application.common.ports.channel import Channel
from tgdb.application.common.ports.clock import Clock
from tgdb.application.common.ports.queque import Queque
from tgdb.application.common.ports.relations import Relations
from tgdb.application.common.ports.shared_horizon import SharedHorizon
from tgdb.application.common.ports.tuples import Tuples
from tgdb.application.common.ports.uuids import UUIDs
from tgdb.application.horizon.commit_transaction import CommitTransaction
from tgdb.application.horizon.output_commits import OutputCommits
from tgdb.application.horizon.output_commits_to_tuples import (
    OutputCommitsToTuples,
)
from tgdb.application.horizon.rollback_transaction import RollbackTransaction
from tgdb.application.horizon.start_transaction import StartTransaction
from tgdb.application.relation.create_relation import CreateRelation
from tgdb.application.view_tuples import ViewTuples
from tgdb.entities.horizon.horizon import Horizon, horizon
from tgdb.entities.horizon.transaction import PreparedCommit
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
from tgdb.infrastructure.pyyaml.conf import Conf
from tgdb.infrastructure.telethon.client_pool import (
    TelegramClientPool,
    loaded_client_pool_from_farm_file,
)
from tgdb.infrastructure.telethon.in_telegram_bytes import InTelegramBytes
from tgdb.infrastructure.telethon.in_telegram_heap import InTelegramHeap
from tgdb.infrastructure.telethon.lazy_message_map import (
    LazyMessageMap,
    lazy_message_map,
)
from tgdb.infrastructure.typenv.envs import Envs


BotPool = NewType("BotPool", TelegramClientPool)
UserBotPool = NewType("UserBotPool", TelegramClientPool)

RelationCache = NewType("RelationCache", InMemoryDb[Relation])


class MainIOProvider(Provider):
    provide_envs = provide(Envs.load, scope=Scope.APP)

    @provide(scope=Scope.APP)
    def provide_conf(self, envs: Envs) -> Conf:
        return Conf.load(envs.conf_path)


class CommonProvider(Provider):
    provide_clock = provide(PerfCounterClock, provides=Clock, scope=Scope.APP)
    provide_uuids = provide(UUIDs4, provides=UUIDs, scope=Scope.APP)
    provide_queque = provide(
        staticmethod(lambda: InMemoryQueque(AsyncQueque())),
        provides=Queque[Sequence[PreparedCommit]],
        scope=Scope.APP
    )
    provide_channel = provide(
        staticmethod(lambda: AsyncMapChannel(AsyncMap())),
        provides=Channel,
        scope=Scope.APP,
    )

    @provide(scope=Scope.APP)
    async def provide_bot_pool(self, conf: Conf) -> AsyncIterator[BotPool]:
        pool = BotPool(loaded_client_pool_from_farm_file(
            conf.clients.bots,
            conf.api.id,
            conf.api.hash,
        ))
        async with pool:
            yield pool

    @provide(scope=Scope.APP)
    async def provide_userbot_pool(
        self, conf: Conf
    ) -> AsyncIterator[UserBotPool]:
        pool = UserBotPool(loaded_client_pool_from_farm_file(
            conf.clients.userbots,
            conf.api.id,
            conf.api.hash,
        ))
        async with pool:
            yield pool

    @provide(scope=Scope.APP)
    def provide_horizon(self, conf: Conf) -> Horizon:
        return horizon(
            0,
            conf.horizon.max_len,
            int(conf.horizon.transaction.max_age_seconds * 1_000_000_000),
        )

    @provide(scope=Scope.APP)
    def provide_shared_horizon(self, horizon: Horizon) -> SharedHorizon:
        return InMemorySharedHorizon(horizon)

    @provide(scope=Scope.APP)
    def provide_lazy_message_map(
        self,
        user_bot_pool: UserBotPool,
        conf: Conf,
    ) -> LazyMessageMap:
        return lazy_message_map(user_bot_pool, conf.message_cache.max_len)

    @provide(scope=Scope.APP)
    def provide_in_telegram_heap(
        self,
        bot_pool: BotPool,
        user_bot_pool: UserBotPool,
        conf: Conf,
        lazy_message_map: LazyMessageMap,
    ) -> InTelegramHeap:
        return InTelegramHeap(
            bot_pool,
            user_bot_pool,
            bot_pool,
            bot_pool,
            conf.heap.chat,
            InTelegramHeap.encoded_tuple_max_len(conf.heap.page.fullness),
            lazy_message_map,
        )

    provide_tuples = provide(
        InTelegramHeapTuples,
        provides=Tuples,
        scope=Scope.APP,
    )

    @provide(scope=Scope.APP)
    def provide_in_memory_buffer[ValueT](
        self,
        conf: Conf,
    ) -> InMemoryBuffer[ValueT]:
        return InMemoryBuffer(
           conf.buffer.overflow.len,
           conf.buffer.overflow.timeout_seconds,
           deque(),
        )

    @provide(scope=Scope.APP)
    async def provide_buffer(
        self,
        conf: Conf,
        bot_pool: BotPool,
        user_bot_pool: UserBotPool,
        buffer: InMemoryBuffer[PreparedCommit]
    ) -> AsyncIterator[Buffer[PreparedCommit]]:
        in_tg_bytes = InTelegramBytes(bot_pool, user_bot_pool, conf.buffer.chat)

        biffer = InTelegramReplicablePreparedCommitBuffer(buffer, in_tg_bytes)

        async with biffer:
            yield biffer

    @provide(scope=Scope.APP)
    def provide_relation_cache(self) -> RelationCache:
        return RelationCache(InMemoryDb())

    @provide(scope=Scope.APP)
    async def provide_relations(
        self,
        conf: Conf,
        bot_pool: BotPool,
        user_bot_pool: UserBotPool,
        relation_cache: RelationCache,
    ) -> AsyncIterator[Relations]:
        in_tg_bytes = InTelegramBytes(
            bot_pool, user_bot_pool, conf.relations.chat
        )
        relations = InTelegramReplicableRelations(in_tg_bytes, relation_cache)

        async with relations:
            yield relations

    provide_commit_transaction = provide(CommitTransaction, scope=Scope.APP)
    provide_output_commits = provide(OutputCommits, scope=Scope.APP)
    provide_output_commits_to_tuples = provide(
        OutputCommitsToTuples, scope=Scope.APP
    )
    provide_rollback_transaction = provide(RollbackTransaction, scope=Scope.APP)
    provide_start_transaction = provide(StartTransaction, scope=Scope.APP)

    provide_create_relations = provide(CreateRelation, scope=Scope.APP)

    provide_view_tuples = provide(ViewTuples, scope=Scope.APP)


main_io_container = make_container(MainIOProvider())
