from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.entities.horizon.transaction import TransactionEffect
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import Tuple


@dataclass(frozen=True)
class OversizedRelationSchemaError(Exception):
    schema_size: int
    schema_max_size: int


class Tuples(ABC):
    @abstractmethod
    async def tuples_with_attribute(
        self,
        relation_number: Number,
        attribute_number: Number,
        attribute_scalar: Scalar,
        /,
    ) -> Sequence[Tuple]: ...

    @abstractmethod
    async def map(self, effects: Sequence[TransactionEffect], /) -> None: ...

    @abstractmethod
    async def map_idempotently(
        self,
        effects: Sequence[TransactionEffect],
        /,
    ) -> None: ...

    @abstractmethod
    async def assert_can_accept_tuples(self, relation: Relation) -> None:
        """
        :raises tgdb.application.relation.ports.relations.OversizedRelationSchemaError:
        """  # noqa: E501
