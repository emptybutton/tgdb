from collections.abc import Sequence
from dataclasses import dataclass

from tgdb.application.common.ports.clock import Clock
from tgdb.application.horizon.ports.shared_horizon import SharedHorizon
from tgdb.application.relation.ports.relations import Relations
from tgdb.application.relation.ports.tuples import Tuples
from tgdb.entities.horizon.transaction import XID
from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import Tuple
from tgdb.entities.relation.tuple_effect import viewed_tuple
from tgdb.entities.relation.versioned_tuple import versioned_tuple


@dataclass(frozen=True)
class ViewTuples:
    shared_horizon: SharedHorizon
    clock: Clock
    tuples: Tuples
    relartions: Relations

    async def __call__(
        self,
        xid: XID | None,
        relation_number: Number,
        attribute_number: Number,
        attribute_scalar: Scalar,
    ) -> Sequence[Tuple]:
        """
        :raises tgdb.application.relation.ports.relations.NoRelationError:
        :raises tgdb.entities.horizon.horizon.NoTransactionError:
        :raises tgdb.entities.horizon.horizon.TransactionCommittingError:
        """

        tuples = await self.tuples.tuples_with_attribute(
            relation_number,
            attribute_number,
            attribute_scalar,
        )
        versioned_tuples = map(versioned_tuple, tuples)
        relation = await self.relartions.relation(relation_number)

        viewed_tuples = (
            viewed_tuple(versioned_tuple, relation)
            for versioned_tuple in versioned_tuples
        )

        if xid is not None:
            async with self.shared_horizon as horizon:
                for viewed_tuple_ in viewed_tuples:
                    time = await self.clock
                    horizon.include(time, xid, viewed_tuple_)

        return tuples
