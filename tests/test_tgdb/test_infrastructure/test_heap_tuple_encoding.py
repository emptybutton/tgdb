from uuid import UUID

from pytest import mark

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import RelationSchemaID
from tgdb.entities.relation.tuple import Tuple, tuple_
from tgdb.infrastructure.heap_tuple_encoding import HeapTupleEncoding


@mark.parametrize(
    "tuple_",
    [
        tuple_(1, 2, 3, tid=UUID(int=0)),
        tuple_(
            -1,
            "-1",
            True,  # noqa: FBT003
            1,
            None,
            UUID(int=1234567098765432),
            tid=UUID(int=0),
        ),
        tuple_(tid=UUID(int=0)),
        tuple_(None, "asd", tid=UUID(int=0)),
        tuple_(None, tid=UUID(int=100000)),
        tuple_(
            None,
            tid=UUID(int=100000),
            relation_schema_id=RelationSchemaID(Number(1000), Number(200_000)),
        ),
    ],
)
def test_isomorphism(tuple_: Tuple) -> None:
    decoded_tuple = HeapTupleEncoding.decoded_tuple(
        HeapTupleEncoding.encoded_tuple(tuple_),
    )

    assert decoded_tuple == tuple_
