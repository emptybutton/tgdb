from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from urllib import parse
from uuid import UUID


type Primitive = bool | int | str | datetime | UUID | StrEnum
type DecodedPrimitive[PrimitiveT: Primitive = Primitive] = (
    Callable[[str, type[Primitive]], Primitive]
)


def encoded_primitive(primitive: Primitive) -> str:
    if isinstance(primitive, bool):
        return str(int(primitive))

    if isinstance(primitive, int):
        return str(primitive)

    if isinstance(primitive, str):
        return parse.quote(primitive)

    if isinstance(primitive, datetime):
        return primitive.isoformat()

    if isinstance(primitive, StrEnum):
        return parse.quote(primitive.value)

    return str(primitive)


def decoded_bool(encoded_value: str, _: object) -> bool | None:
    match encoded_value:
        case "1":
            return True
        case "0":
            return False
        case _:
            return None


def decoded_int(
    encoded_value: str,
    _: object = None,
) -> int | None:
    try:
        return int(encoded_value)
    except ValueError:
        return None


def decoded_str(
    encoded_value: str,
    _: object = None,
) -> str:
    return parse.unquote(encoded_value)


def decoded_datetime(
    encoded_value: str,
    _: object = None,
) -> datetime | None:
    try:
        return datetime.fromisoformat(encoded_value)
    except ValueError:
        return None


def decoded_uuid(
    encoded_value: str,
    _: object = None,
) -> UUID | None:
    try:
        return UUID(hex=encoded_value)
    except ValueError:
        return None


def decoded_str_enum(
    encoded_value: str,
    enum_type: type[StrEnum]
) -> StrEnum | None:
    try:
        return enum_type(parse.unquote(encoded_value))
    except ValueError:
        return None


decoded_primitive_func_by_primitive_type: dict[
    type[Primitive], DecodedPrimitive
]
decoded_primitive_func_by_primitive_type = {
    bool: decoded_bool,
    int: decoded_int,
    str: decoded_str,
    datetime: decoded_datetime,
    UUID: decoded_uuid,
    StrEnum: decoded_str_enum
}


def decoded_primitive[PrimitiveT: Primitive](
    encoded_value: str, type_: type[PrimitiveT]
) -> PrimitiveT:
    decoded = decoded_primitive_func_by_primitive_type[
        encoded_value
    ]

    return decoded(encoded_value, type_)  # type: ignore[arg-type]
