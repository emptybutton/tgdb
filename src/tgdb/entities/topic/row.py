from dataclasses import dataclass

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar


@dataclass(frozen=True)
class RowID:
    topic_number: Number
    scalar: Scalar
