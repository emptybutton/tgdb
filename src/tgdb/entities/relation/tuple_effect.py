from dataclasses import dataclass

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation, RelationSchemaID
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID, Tuple
from tgdb.entities.relation.versioned_tuple import VersionedTuple


@dataclass(frozen=True)
class JustViewedTuple:
    tid: TID

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        return effect


@dataclass(frozen=True)
class NewTuple:
    tuple: Tuple

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        match effect:
            case JustViewedTuple():
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

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        match effect:
            case JustViewedTuple():
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

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        match effect:
            case JustViewedTuple():
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

    def __and__(self, effect: "TupleEffect") -> "TupleEffect":
        match effect:
            case (
                JustViewedTuple()
                | MutatedTuple()
                | DeletedTuple()
                | MigratedTuple()
            ):
                return self
            case NewTuple(tuple):
                return MutatedTuple(tuple)


@dataclass(frozen=True)
class InvalidRelationTupleError(Exception):
    tid: TID
    scalars: tuple[Scalar, ...]
    relation_number: Number


type TupleEffect = (
    NewTuple
    | JustViewedTuple
    | MutatedTuple
    | MigratedTuple
    | DeletedTuple
)


def relation_tuple(tuple: Tuple, relation: Relation) -> Tuple:
    """
    :raises tgdb.entities.relation.tuple_effect.InvalidRelationTupleError:
    """

    relation_last_version = relation.last_version()

    if not tuple.matches(relation_last_version.schema):
        raise InvalidRelationTupleError(
            tuple.tid, tuple.scalars, relation.number()
        )

    return tuple


def constructed_relation_tuple(
    tid: TID,
    scalars: tuple[Scalar, ...],
    relation: Relation,
) -> Tuple:
    """
    :raises tgdb.entities.relation.tuple_effect.InvalidRelationTupleError:
    """

    relation_last_version = relation.last_version()
    relation_last_version_id = RelationSchemaID(
        relation.number(), relation_last_version.number
    )

    tuple = Tuple(tid, relation_last_version_id, scalars)

    return relation_tuple(tuple, relation)


def new_tuple(
    tid: TID,
    scalars: tuple[Scalar, ...],
    relation: Relation,
) -> NewTuple:
    """
    :raises tgdb.entities.relation.tuple_effect.InvalidRelationTupleError:
    """

    return NewTuple(constructed_relation_tuple(tid, scalars, relation))


def mutated_tuple(
    tid: TID,
    scalars: tuple[Scalar, ...],
    relation: Relation,
) -> MutatedTuple:
    """
    :raises tgdb.entities.relation.tuple_effect.InvalidRelationTupleError:
    """

    return MutatedTuple(constructed_relation_tuple(tid, scalars, relation))


def deleted_tuple(tid: TID) -> DeletedTuple:
    return DeletedTuple(tid)


type ViewedTuple = JustViewedTuple | MigratedTuple


def viewed_tuple(tuple: VersionedTuple, relation: Relation) -> ViewedTuple:
    """
    :raises tgdb.entities.relation.tuple_effect.InvalidRelationTupleError:
    """

    last_version = relation_tuple(tuple.last_version(), relation)
    old_versions = tuple.old_versions()

    if not last_version.matches(relation.last_version().schema):
        raise InvalidRelationTupleError(
            last_version.tid, last_version.scalars, relation.number()
        )

    if old_versions:
        return MigratedTuple(last_version)

    return JustViewedTuple(tuple.last_version().tid)
