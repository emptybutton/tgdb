from dishka import Provider, Scope, make_async_container, provide

from tgdb import __version__
from tgdb.application.common.ports.relation_views import RelationViews
from tgdb.application.horizon.output_commits import OutputCommits
from tgdb.application.horizon.output_commits_to_tuples import (
    OutputCommitsToTuples,
)
from tgdb.application.relation.view_all_relations import ViewAllRelations
from tgdb.application.relation.view_relation import ViewRelation
from tgdb.main.common.di import CommonProvider, RelationCache
from tgdb.presentation.adapters.relation_views import (
    RelationSchemasFromInMemoryDbAsRelationViews,
)
from tgdb.presentation.fastapi.common.app import (
    FastAPIAppCoroutines,
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
        self, relation_cache: RelationCache
    ) -> RelationViews[RelationListSchema, RelationSchema | None]:
        return RelationSchemasFromInMemoryDbAsRelationViews(relation_cache)

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
    ) -> FastAPIAppCoroutines:
        return FastAPIAppCoroutines((
            output_commits(),
            output_commits_to_tuples(),
        ))

    @provide(scope=Scope.APP)
    def provide_fast_api_app_routers(self) -> FastAPIAppRouters:
        return FastAPIAppRouters(all_routers)

    @provide(scope=Scope.APP)
    def provide_fast_api_app_version(self) -> FastAPIAppVersion:
        return FastAPIAppVersion(__version__)


server_container = make_async_container(CommonProvider(), FastAPIProvider())
