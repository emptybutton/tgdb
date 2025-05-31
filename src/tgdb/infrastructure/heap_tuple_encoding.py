from enum import Enum

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import RelationVersionID
from tgdb.entities.relation.scalar import Scalar
from tgdb.entities.relation.tuple import TID, Tuple
from tgdb.infrastructure.primitive_encoding import (
    ReversibleTranslationTable,
    decoded_int,
    decoded_primitive_with_type,
    decoded_uuid,
    encoded_int,
    encoded_primitive_with_type,
    encoded_uuid,
)


class Separator(Enum):
    first_level = "\uffff"
    second_level = "\ufffe"


heap_tuple_table = ReversibleTranslationTable({
    ord(separator.value): None for separator in Separator
})


class HeapTupleEncoding:
    @staticmethod
    def encoded_tuple(tuple_: Tuple) -> str:
        encoded_metadata = HeapTupleMetadataEncoding.encoded_metadata(tuple_)
        encoded_attributes = (
            HeapTupleAttributeEncoding.encoded_attribute(
                int(tuple_.relation_version_id.relation_number),
                attribute_number,
                tuple_[attribute_number],
            )
            for attribute_number in range(len(tuple_))
        )

        encoded_tuple_without_end = Separator.first_level.value.join(
            (encoded_metadata, *encoded_attributes)
        )
        return f"{encoded_tuple_without_end}{Separator.first_level.value}"

    @staticmethod
    def decoded_tuple(encoded_tuple: str) -> Tuple:
        encoded_metadata, *encoded_attributes = (
            encoded_tuple.split(Separator.first_level.value)
        )

        tid, relation_version_id = HeapTupleMetadataEncoding.decoded_metadata(
            encoded_metadata
        )
        scalars = tuple(map(
            HeapTupleAttributeEncoding.decoded_scalar, encoded_attributes
        ))

        return Tuple(tid, relation_version_id, scalars)

    @staticmethod
    def id_of_encoded_tuple_with_attribute(
        relation_number: int,
        attribute_number: int,
        attribute_scalar: Scalar,
    ) -> str:
        encoded_attribute = HeapTupleAttributeEncoding.encoded_attribute(
            relation_number,
            attribute_number,
            attribute_scalar
        )

        return (
            f"{Separator.first_level.value}"
            f"{encoded_attribute}"
            f"{Separator.first_level.value}"
        )


type HeapTupleMetadata = tuple[TID, RelationVersionID]


class HeapTupleMetadataEncoding:
    @staticmethod
    def encoded_metadata(tuple: Tuple) -> str:
        return Separator.second_level.value.join((
            encoded_int(int(tuple.relation_version_id.relation_version_number)),
            encoded_int(int(tuple.relation_version_id.relation_number)),
            encoded_uuid(tuple.tid),
        ))

    @staticmethod
    def decoded_metadata(encoded_metadata: str) -> HeapTupleMetadata:
        encoded_version_number, encoded_relation_number, encoded_tid = (
            encoded_metadata.split(Separator.second_level.value)
        )

        relation_version_number = (Number(decoded_int(encoded_version_number)))
        relation_number = Number(decoded_int(encoded_relation_number))
        tid = decoded_uuid(encoded_tid)

        version_id = RelationVersionID(relation_number, relation_version_number)

        return tid, version_id


class HeapTupleAttributeEncoding:
    @staticmethod
    def encoded_attribute(
        relation_number: int,
        attribute_number: int,
        scalar: Scalar,
    ) -> str:
        return Separator.second_level.value.join((
            encoded_int(relation_number),
            encoded_int(attribute_number),
            encoded_primitive_with_type(scalar, heap_tuple_table),
        ))

    @staticmethod
    def decoded_scalar(encoded_attribute: str) -> Scalar:
        _, _, encoded_scalar = encoded_attribute.split(
            Separator.second_level.value
        )

        return decoded_primitive_with_type(encoded_scalar, heap_tuple_table)
