from tgdb.presentation.fastapi.common.routes.healthcheck import (
    healthcheck_router,
)
from tgdb.presentation.fastapi.horizon.routers import horizon_routers
from tgdb.presentation.fastapi.relation.routers import relation_routers


_monitoring_routers = (healthcheck_router,)


all_routers = (
    *_monitoring_routers,
    *horizon_routers,
    *relation_routers,
)
