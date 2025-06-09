from pydantic import BaseModel

from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID, Tuple


class TupleSchema(BaseModel):
    tid: TID
    scalars: tuple[Scalar, ...]

    @classmethod
    def of(cls, tuple_: Tuple) -> "TupleSchema":
        return TupleSchema(tid=tuple_.tid, scalars=tuple_.scalars)
