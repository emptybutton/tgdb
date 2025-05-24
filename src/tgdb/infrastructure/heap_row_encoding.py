from tgdb.entities.row import Row, RowAttribute, Schema
from tgdb.infrastructure.primitive_encoding import encoded_primitive


def encoded_heap_row(row: Row) -> str:
    encoded_attributes = (
        encoded_heap_row_attribute(row.schema, attribute_number, attribute)
        for attribute_number, attribute in enumerate(row)
    )
    return f"|{"|".join(encoded_attributes)}|"


def encoded_heap_row_attribute(
    schema: Schema,
    attribute_number: int,
    attribute: RowAttribute,
) -> str:
    header = encoded_heap_row_attribute_header(attribute_number, schema)
    body = encoded_primitive(attribute)

    return f"{header}{body}"


def encoded_heap_row_attribute_header(
    attribute_number: int, schema: Schema
) -> str:
    return f"{encoded_primitive(schema)}#{attribute_number}="
