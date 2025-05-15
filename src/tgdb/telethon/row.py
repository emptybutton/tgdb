from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, cast, overload

from effect import IdentifiedValue, LifeCycle

from tgdb.telethon.primitive import (
    Primitive,
    decoded_primitive,
    encoded_primitive,
)


type RowAttribute = Primitive


@dataclass(frozen=True)
class RowSchema(Sequence[type[RowAttribute]]):
    name: str
    id_type: type[RowAttribute]
    body_types: tuple[type[RowAttribute], ...] = tuple()

    def __iter__(self) -> Iterator[type[RowAttribute]]:
        yield self.id_type  # type: ignore[misc]
        yield from self.body_types

    @overload
    def __getitem__(self, index: int, /) -> type[RowAttribute]: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[type[RowAttribute]]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[type[RowAttribute]] | type[RowAttribute]:
        return tuple(self)[value]

    def __len__(self) -> int:
        return len(self.body_types) + 1


class RowSchemaError(Exception):
    def __init__(self, schema: RowSchema) -> None:
        self.schema = schema
        super().__init__()


@dataclass(frozen=True)
class Row(IdentifiedValue[RowAttribute], Sequence[RowAttribute]):
    body: tuple[RowAttribute, ...]
    schema: RowSchema

    def __post_init__(self) -> None:
        for attribute_and_type in zip(self, self.schema, strict=False):
            if len(attribute_and_type) != 2:  # noqa: PLR2004
                raise RowSchemaError(self.schema)

            attribute, type = attribute_and_type

            if not isinstance(attribute, type):
                raise RowSchemaError(self.schema)

    def __iter__(self) -> Iterator[RowAttribute]:
        yield self.id
        yield from self.body

    @overload
    def __getitem__(self, index: int, /) -> RowAttribute: ...

    @overload
    def __getitem__(
        self, slice_: "slice[Any, Any, Any]", /
    ) -> Sequence[RowAttribute]: ...

    def __getitem__(
        self, value: "int | slice[Any, Any, Any]", /
    ) -> Sequence[RowAttribute] | RowAttribute:
        return tuple(self)[value]

    def __len__(self) -> int:
        return len(self.body) + 1


def dynamic_schema(
    schema_name: str, row_id: RowAttribute, row_body: tuple[RowAttribute, ...]
) -> RowSchema:
    return RowSchema(schema_name, type(row_id), tuple(map(type, row_body)))


def row_with_dynamic_schema(
    schema_name: str, row_id: RowAttribute, row_body: tuple[RowAttribute, ...]
) -> Row:
    return Row(row_id, row_body, dynamic_schema(schema_name, row_id, row_body))


def row_from_attributes(
    attrs: tuple[RowAttribute, ...],
    schema: RowSchema,
) -> Row:
    if not attrs:
        raise RowSchemaError(schema)

    id, *body = attrs

    return Row(id, tuple(body), schema)


attribute_separator = " "
attrubute_header_end = "="
max_encoded_row_size = 4000


class TooLargeEncodedRowError(Exception): ...


def encoded_row(row: Row) -> str:
    encoded_attributes = (
        encoded_attribute(row.schema, attribute_number, attribute)
        for attribute_number, attribute in enumerate(row)
    )
    encoded_row_ = attribute_separator.join(encoded_attributes)

    if len(encoded_row_) > max_encoded_row_size:
        raise TooLargeEncodedRowError

    return encoded_row_


def encoded_attribute(
    schema: RowSchema,
    attribute_number: int,
    attribute: RowAttribute,
) -> str:
    header = encoded_attribute_header(attribute_number, schema)
    body = encoded_primitive(attribute)

    return f"{header}{body}"


def encoded_attribute_header(
    attribute_number: int,
    schema: RowSchema,
) -> str:
    return (
        f"{schema.name}[{attribute_number}]{attrubute_header_end}"
    )


def decoded_attribute(
    schema: RowSchema,
    attribute_number: int,
    encoded_attribute: str,
) -> RowAttribute | None:
    header_end_index = encoded_attribute.find(attrubute_header_end)

    if header_end_index == -1:
        return None

    header = encoded_attribute[:header_end_index + 1]
    body = encoded_attribute[header_end_index + 1:]

    excepted_header = encoded_attribute_header(attribute_number, schema)

    if header != excepted_header:
        return None

    attribute_type = schema[attribute_number]

    return decoded_primitive(body, attribute_type)


def decoded_row(
    schema: RowSchema, encoded_row: str
) -> Row:
    encoded_attributes = encoded_row.split(attribute_separator)

    attributes = tuple(
        decoded_attribute(
            schema,  # type: ignore[arg-type]
            attribute_number,
            encoded_attribute,
        )
        for attribute_number, encoded_attribute in enumerate(encoded_attributes)
    )

    if any(attribute is None for attribute in attributes):
        raise RowSchemaError(schema)

    return row_from_attributes(
        cast(tuple[RowAttribute, ...], attributes),
        schema,
    )


def schema_name_of_encoded_row(encoded_row: str) -> str | None:
    schema_name_end_index = encoded_row.find("[")

    if schema_name_end_index == -1:
        return None

    return encoded_row[:schema_name_end_index]


def processed_encoded_row(
    processed_row: Callable[[Row], LifeCycle[Row]],
    encoded_row_: str,
    encoded_row_schema: RowSchema
) -> LifeCycle[Row]:

    return processed_row(decoded_row(encoded_row_schema, encoded_row_))


def map_encoded_row(
    next_row: Callable[[Row], Row],
    encoded_row_: str,
    encoded_row_schema: RowSchema
) -> str:
    next_row_ = next_row(decoded_row(encoded_row_schema, encoded_row_))

    return encoded_row(next_row_)  # type: ignore[arg-type]


def query_text(
    schema: RowSchema, attribute_number: int, attribute: RowAttribute | None
) -> str:
    if attribute is None:
        return encoded_attribute_header(attribute_number, schema)

    return encoded_attribute(schema, attribute_number, attribute)
