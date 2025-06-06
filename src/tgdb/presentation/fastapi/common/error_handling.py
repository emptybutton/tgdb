from fastapi import FastAPI

from tgdb.presentation.fastapi.horizon.error_handling import (
    add_horizon_error_handling,
)
from tgdb.presentation.fastapi.relation.error_handling import (
    add_relation_error_handling,
)


def add_error_handling(app: FastAPI) -> None:
    add_horizon_error_handling(app)
    add_relation_error_handling(app)
