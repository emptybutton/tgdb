from dishka import Provider, Scope, make_async_container, provide

from tgdb import __version__
from tgdb.application.horizon.output_commits import OutputCommits
from tgdb.application.horizon.output_commits_to_tuples import (
    OutputCommitsToTuples,
)
from tgdb.application.relation.ports.relation_views import RelationViews
from tgdb.application.relation.view_all_relations import ViewAllRelations
from tgdb.application.relation.view_relation import ViewRelation
from tgdb.infrastructure.adapters.relations import InTelegramReplicableRelations
from tgdb.main.common.di import CommonProvider, MainIOProvider
from tgdb.presentation.adapters.relation_views import (
    RelationSchemasFromInMemoryDbAsRelationViews,
)
from tgdb.presentation.fastapi.common.app import (
    FastAPIAppBackground,
    FastAPIAppRouters,
    FastAPIAppVersion,
)
from tgdb.presentation.fastapi.common.routers import all_routers
from tgdb.presentation.fastapi.relation.schemas.relation import (
    RelationListSchema,
    RelationSchema,
)


class FastAPIProvider(Provider):
    @provide(scope=Scope.APP)
    def provide_relation_views(
        self,
        relations: InTelegramReplicableRelations,
    ) -> RelationViews[RelationListSchema, RelationSchema | None]:
        return RelationSchemasFromInMemoryDbAsRelationViews(relations.cache())

    provide_view_relation = provide(
        ViewRelation[RelationListSchema, RelationSchema | None],
        scope=Scope.APP,
    )
    provide_view_all_relations = provide(
        ViewAllRelations[RelationListSchema, RelationSchema | None],
        scope=Scope.APP,
    )

    @provide(scope=Scope.APP)
    def provide_fast_api_app_coroutines(
        self,
        output_commits_to_tuples: OutputCommitsToTuples,
        output_commits: OutputCommits,
    ) -> FastAPIAppBackground:
        return FastAPIAppBackground((
            output_commits,
            output_commits_to_tuples,
        ))

    @provide(scope=Scope.APP)
    def provide_fast_api_app_routers(self) -> FastAPIAppRouters:
        return FastAPIAppRouters(all_routers)

    @provide(scope=Scope.APP)
    def provide_fast_api_app_version(self) -> FastAPIAppVersion:
        return FastAPIAppVersion(__version__)


server_container = make_async_container(
    MainIOProvider(),
    CommonProvider(),
    FastAPIProvider(),
)
