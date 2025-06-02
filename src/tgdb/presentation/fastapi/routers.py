from tgdb.presentation.fastapi.routes.commit_transaction import (
    commit_transaction_router,
)
from tgdb.presentation.fastapi.routes.healthcheck import healthcheck_router
from tgdb.presentation.fastapi.routes.rollback_transaction import (
    rollback_transaction_router,
)
from tgdb.presentation.fastapi.routes.start_transaction import (
    start_transaction_router,
)


_monitoring_routers = (
    healthcheck_router,
    rollback_transaction_router,
    commit_transaction_router,
)


_transaction_routers = (
    start_transaction_router,
    rollback_transaction_router,
    commit_transaction_router,
)


all_routers = (
    *_monitoring_routers,
    *_transaction_routers,
)
