from tgdb.presentation.fastapi.relation.routes.view_all_relations import (
    view_all_relations_router,
)
from tgdb.presentation.fastapi.relation.routes.view_relation import (
    view_relation_router,
)


relation_routers = (
    view_all_relations_router,
    view_relation_router,
)
