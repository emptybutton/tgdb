from tgdb.entities.horizon.transaction import TransactionScalarEffect
from tgdb.entities.relation.tuple_effect import (
    DeletedTuple,
    MigratedTuple,
    MutatedTuple,
    NewTuple,
)
from tgdb.infrastructure.pydantic.relation.tuple_effect import (
    EncodableDeletedTuple,
    EncodableMigratedTuple,
    EncodableMutatedTuple,
    EncodableNewTuple,
)


type EncodableTransactionScalarEffect = (
    EncodableNewTuple
    | EncodableMutatedTuple
    | EncodableMigratedTuple
    | EncodableDeletedTuple
)


def encodable_transaction_scalar_effect(
    effect: TransactionScalarEffect,
) -> EncodableTransactionScalarEffect:
    match effect:
        case NewTuple():
            return EncodableNewTuple.of(effect)

        case MutatedTuple():
            return EncodableMutatedTuple.of(effect)

        case MigratedTuple():
            return EncodableMigratedTuple.of(effect)

        case DeletedTuple():
            return EncodableDeletedTuple.of(effect)
