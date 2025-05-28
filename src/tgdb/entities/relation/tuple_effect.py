from dataclasses import dataclass

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation, RelationVersionID
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID, Tuple
from tgdb.entities.relation.versioned_tuple import VersionedTuple


@dataclass(frozen=True)
class ViewedTuple:
    tid: TID

    def __and__(self, effect: "TupleOkEffect") -> "TupleOkEffect":
        return effect


@dataclass(frozen=True)
class NewTuple:
    tuple: Tuple

    def __and__(self, effect: "TupleOkEffect") -> "TupleOkEffect":
        match effect:
            case ViewedTuple():
                return self
            case MutatedTuple(tuple) | MigratedTuple(tuple):
                return NewTuple(tuple)
            case NewTuple() | DeletedTuple():
                return effect

    @property
    def tid(self) -> TID:
        return self.tuple.tid


@dataclass(frozen=True)
class MutatedTuple:
    tuple: Tuple

    def __and__(self, effect: "TupleOkEffect") -> "TupleOkEffect":
        match effect:
            case ViewedTuple():
                return self
            case NewTuple(tuple):
                return MutatedTuple(tuple)
            case MutatedTuple() | DeletedTuple() | MigratedTuple():
                return effect

    @property
    def tid(self) -> TID:
        return self.tuple.tid


@dataclass(frozen=True)
class MigratedTuple:
    tuple: Tuple

    def __and__(self, effect: "TupleOkEffect") -> "TupleOkEffect":
        match effect:
            case ViewedTuple():
                return self
            case NewTuple(tuple):
                return MutatedTuple(tuple)
            case MutatedTuple() | DeletedTuple() | MigratedTuple():
                return effect

    @property
    def tid(self) -> TID:
        return self.tuple.tid


@dataclass(frozen=True)
class DeletedTuple:
    tid: TID

    def __and__(self, effect: "TupleOkEffect") -> "TupleOkEffect":
        match effect:
            case (
                ViewedTuple()
                | MutatedTuple()
                | DeletedTuple()
                | MigratedTuple()
            ):
                return self
            case NewTuple(tuple):
                return MutatedTuple(tuple)


@dataclass(frozen=True)
class InvalidTuple:
    tid: TID | None
    scalars: tuple[Scalar, ...] | None
    relation_number: Number | None


type TupleOkEffect = (
    NewTuple
    | ViewedTuple
    | MutatedTuple
    | MigratedTuple
    | DeletedTuple
)
type TupleEffect = TupleOkEffect | InvalidTuple


def relation_last_version_tuple(
    tid: TID | None,
    scalars: tuple[Scalar, ...] | None,
    relation: Relation | None,
) -> Tuple | InvalidTuple:
    if tid is None or scalars is None or relation is None:
        relation_number = None if relation is None else relation.number()
        return InvalidTuple(tid, scalars, relation_number)

    relation_last_version = relation.last_version()
    relation_last_version_id = RelationVersionID(
        relation.number(), relation_last_version.number
    )

    tuple = Tuple(tid, relation_last_version_id, scalars)

    if not tuple.matches(relation_last_version.schema):
        return InvalidTuple(tid, scalars, relation.number())

    return tuple


def new_tuple(
    tid: TID | None,
    scalars: tuple[Scalar, ...] | None,
    relation: Relation | None,
) -> NewTuple | InvalidTuple:
    tuple = relation_last_version_tuple(tid, scalars, relation)

    if isinstance(tuple, InvalidTuple):
        return tuple

    return NewTuple(tuple)


def mutated_tuple(
    tid: TID | None,
    scalars: tuple[Scalar, ...] | None,
    relation: Relation | None,
) -> MutatedTuple | InvalidTuple:
    tuple = relation_last_version_tuple(tid, scalars, relation)

    if isinstance(tuple, InvalidTuple):
        return tuple

    return MutatedTuple(tuple)


def deleted_tuple(tid: TID | None) -> DeletedTuple | InvalidTuple:
    if tid is None:
        return InvalidTuple(None, None, None)

    return DeletedTuple(tid)


def viewed_tuple(
    tuple: VersionedTuple | None, relation: Relation | None
) -> ViewedTuple | MigratedTuple | InvalidTuple:
    if tuple is None or relation is None:
        return InvalidTuple(None, None, None)

    last_version = tuple.last_version()
    old_versions = tuple.old_versions()

    if not last_version.matches(relation.last_version().schema):
        return InvalidTuple(
            last_version.tid, last_version.scalars, relation.number()
        )

    if old_versions:
        return MigratedTuple(last_version)

    return ViewedTuple(tuple.last_version().tid)
