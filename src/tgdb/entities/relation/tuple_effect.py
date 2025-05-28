from dataclasses import dataclass

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
    tuple: Tuple

    @property
    def tid(self) -> TID:
        return self.tuple.tid


type TupleOkEffect = (
    NewTuple
    | ViewedTuple
    | MutatedTuple
    | MigratedTuple
    | DeletedTuple
)
type TupleEffect = TupleOkEffect | InvalidTuple


def relation_last_version_tuple(
    tid: TID, scalars: tuple[Scalar, ...], relation: Relation
) -> Tuple | InvalidTuple:
    relation_last_version = relation.last_version()
    relation_last_version_id = RelationVersionID(
        relation.number(), relation_last_version.number
    )

    tuple = Tuple(tid, relation_last_version_id, scalars)

    if not tuple.matches(relation_last_version.schema):
        return InvalidTuple(tuple)

    return tuple


def new_tuple(
    tid: TID, scalars: tuple[Scalar, ...], relation: Relation
) -> NewTuple | InvalidTuple:
    tuple = relation_last_version_tuple(tid, scalars, relation)

    if isinstance(tuple, InvalidTuple):
        return tuple

    return NewTuple(tuple)


def mutated_tuple(
    tid: TID, scalars: tuple[Scalar, ...], relation: Relation
) -> MutatedTuple | InvalidTuple:
    tuple = relation_last_version_tuple(tid, scalars, relation)

    if isinstance(tuple, InvalidTuple):
        return tuple

    return MutatedTuple(tuple)


def deleted_tuple(tid: TID) -> DeletedTuple:
    return DeletedTuple(tid)


def viewed_tuple(
    tuple: VersionedTuple, relation: Relation
) -> ViewedTuple | MigratedTuple | InvalidTuple:
    last_version = tuple.last_version()
    old_versions = tuple.old_versions()

    if not last_version.matches(relation.last_version().schema):
        return InvalidTuple(last_version)

    if old_versions:
        return MigratedTuple(last_version)

    return ViewedTuple(tuple.last_version().tid)
