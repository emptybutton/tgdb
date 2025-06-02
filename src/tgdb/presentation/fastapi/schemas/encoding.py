from abc import ABC, abstractmethod
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, PositiveInt

from tgdb.application.common.operator import (
    DeletedTupleOperator,
    MutatedTupleOperator,
    NewTupleOperator,
    Operator,
)
from tgdb.entities.horizon.claim import Claim
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.domain import (
    Domain,
    IntDomain,
    SetDomain,
    StrDomain,
    TypeDomain,
)
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.schema import Schema
from tgdb.entities.relation.tuple import TID, Tuple


class EncodingSchema[ValueT](BaseModel, ABC):
    @abstractmethod
    def decoded(self) -> ValueT: ...
