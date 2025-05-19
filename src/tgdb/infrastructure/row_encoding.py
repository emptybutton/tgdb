from datetime import datetime
from urllib import parse
from uuid import UUID

from tgdb.entities.row import Row, RowAttribute, Schema


attribute_separator = " "
attrubute_header_end = "="
max_encoded_row_size = 4000


class TooLargeEncodedRowError(Exception): ...


def encoded_row(row: Row) -> str:
    """
    :raises tgdb.infrastructure.row_encoding.TooLargeEncodedRowError:
    """

    encoded_attributes = (
        encoded_attribute(row.schema, attribute_number, attribute)
        for attribute_number, attribute in enumerate(row)
    )
    encoded_row_ = attribute_separator.join(encoded_attributes)

    if len(encoded_row_) > max_encoded_row_size:
        raise TooLargeEncodedRowError

    return encoded_row_


def encoded_attribute_body(attr: RowAttribute) -> str:
    match attr:
        case bool():
            return str(int(attr))
        case int():
            return str(attr)
        case str():
            return parse.quote(attr)
        case datetime():
            return attr.isoformat()
        case None:
            return "@"
        case UUID():
            return attr.hex


def encoded_attribute(
    schema: Schema,
    attribute_number: int,
    attribute: RowAttribute,
) -> str:
    header = encoded_attribute_header(attribute_number, schema)
    body = encoded_attribute_body(attribute)

    return f"{header}{body}"


def encoded_attribute_header(attribute_number: int, schema: Schema) -> str:
    return f"{schema}[{attribute_number}]{attrubute_header_end}"
