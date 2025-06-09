from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.domain import (
    BoolDomain,
    DatetimeDomain,
    Domain,
    IntDomain,
    StrDomain,
    UuidDomain,
)
from tgdb.entities.relation.relation import RelationSchemaID
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.schema import Schema
from tgdb.entities.relation.tuple import TID, Tuple
from tgdb.infrastructure.primitive_encoding import (
    ReversibleTranslationTable,
    decoded_int,
    decoded_primitive_with_type,
    decoded_uuid,
    encoded_int,
    encoded_primitive_with_type,
    encoded_primitive_without_type,
    encoded_uuid,
)


class Separator(Enum):
    top_tuple = "\uffff"
    top_metadata = "\ufffe"
    top_attribute = "\ufffd"


heap_tuple_table = ReversibleTranslationTable({
    ord(separator.value): None for separator in Separator
})


class HeapTupleEncoding:
    @staticmethod
    def largest_tuple(
        schema: Schema,
        schema_id: RelationSchemaID,
    ) -> Tuple:
        xid, schema_id = _HeapTupleMetadataEncoding.largest_metadata(schema_id)
        scalars = map(_HeapTupleAttributeEncoding.largest_scalar, schema)

        return Tuple(xid, schema_id, tuple(scalars))

    @staticmethod
    def encoded_tuple(tuple_: Tuple) -> str:
        encoded_metadata = _HeapTupleMetadataEncoding.encoded_metadata(
            int(tuple_.relation_schema_id.relation_number),
            int(tuple_.relation_schema_id.relation_version_number),
            tuple_.tid,
        )
        encoded_attributes = (
            _HeapTupleAttributeEncoding.encoded_attribute(
                int(tuple_.relation_schema_id.relation_number),
                attribute_number,
                tuple_[attribute_number],
            )
            for attribute_number in range(len(tuple_))
        )

        return Separator.top_tuple.value.join((
            encoded_metadata,
            *encoded_attributes,
        ))

    @staticmethod
    def decoded_tuple(encoded_tuple: str) -> Tuple:
        encoded_metadata, *encoded_attributes = encoded_tuple.split(
            Separator.top_tuple.value,
        )

        tid, relation_schema_id = _HeapTupleMetadataEncoding.decoded_metadata(
            encoded_metadata,
        )
        scalars = tuple(
            map(_HeapTupleAttributeEncoding.decoded_scalar, encoded_attributes),
        )

        return Tuple(tid, relation_schema_id, scalars)

    @staticmethod
    def id_of_encoded_tuple_with_attribute(
        relation_number: int,
        attribute_number: int,
        attribute_scalar: Scalar,
    ) -> str:
        return _HeapTupleAttributeEncoding.encoded_attribute(
            relation_number,
            attribute_number,
            attribute_scalar,
        )

    @staticmethod
    def id_of_encoded_tuple_with_tid(tid: TID) -> str:
        return _HeapTupleMetadataEncoding.id_of_encoded_tuple_with_tid(tid)


type _HeapTupleMetadata = tuple[TID, RelationSchemaID]


class _HeapTupleMetadataEncoding:
    @staticmethod
    def encoded_metadata(
        relation_number: int,
        relation_version_number: int,
        tid: TID,
    ) -> str:
        return Separator.top_metadata.value.join((
            encoded_int(relation_version_number),
            encoded_int(relation_number),
            encoded_uuid(tid),
        ))

    @staticmethod
    def decoded_metadata(encoded_metadata: str) -> _HeapTupleMetadata:
        encoded_version_number, encoded_relation_number, encoded_tid = (
            encoded_metadata.split(Separator.top_metadata.value)
        )

        relation_version_number = Number(decoded_int(encoded_version_number))
        relation_number = Number(decoded_int(encoded_relation_number))
        tid = decoded_uuid(encoded_tid)

        schema_id = RelationSchemaID(relation_number, relation_version_number)

        return tid, schema_id

    @staticmethod
    def id_of_encoded_tuple_with_tid(tid: TID) -> str:
        return f"{Separator.top_metadata.value}{encoded_uuid(tid)}"

    @staticmethod
    def largest_metadata(schema_id: RelationSchemaID) -> _HeapTupleMetadata:
        return UUID(int=0), schema_id


class _HeapTupleAttributeEncoding:
    @staticmethod
    def encoded_attribute(
        relation_number: int,
        attribute_number: int,
        scalar: Scalar,
    ) -> str:
        return Separator.top_attribute.value.join((
            encoded_int(relation_number),
            encoded_int(attribute_number),
            encoded_primitive_with_type(scalar, heap_tuple_table),
        ))

    @staticmethod
    def decoded_scalar(encoded_attribute: str) -> Scalar:
        _, _, encoded_scalar = encoded_attribute.split(
            Separator.top_attribute.value,
        )

        return decoded_primitive_with_type(encoded_scalar, heap_tuple_table)

    @staticmethod
    def largest_scalar(domain: Domain) -> Scalar:
        match domain:
            case IntDomain():
                return max(domain.min, domain.max, key=lambda it: len(str(it)))
            case StrDomain():
                return "x" * domain.max_len
            case BoolDomain():
                return True
            case DatetimeDomain():
                tzinfo = timezone(timedelta(hours=14))
                return datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=tzinfo)
            case UuidDomain():
                return UUID(int=0)
            case tuple():
                return max(
                    domain,
                    key=lambda it: (
                        encoded_primitive_without_type(it, heap_tuple_table)
                    ),
                )
