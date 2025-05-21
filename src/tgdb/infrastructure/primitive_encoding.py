from datetime import datetime
from urllib import parse
from uuid import UUID


type Primitive = None | bool | int | str | datetime | UUID  # noqa: RUF036


def encoded_primitive(primitive: Primitive) -> str:
    match primitive:
        case bool():
            return str(int(primitive))
        case int():
            return str(primitive)
        case str():
            return parse.quote(primitive)
        case datetime():
            return primitive.isoformat()
        case None:
            return "^"
        case UUID():
            return primitive.hex


def decoded_bool(encoded_value: str) -> bool:
    match encoded_value:
        case "1":
            return True
        case "0":
            return False
        case _:
            raise ValueError


def decoded_int(encoded_value: str) -> int:
    return int(encoded_value)


def decoded_str(encoded_value: str) -> str:
    return parse.unquote(encoded_value)


def decoded_datetime(encoded_value: str) -> datetime:
    return datetime.fromisoformat(encoded_value)


def decoded_uuid(encoded_value: str) -> UUID:
    return UUID(hex=encoded_value)


def decoded_none(encoded_value: str) -> None:
    assert encoded_value == "^"


_decoding_by_type = {
    bool: decoded_bool,
    int: decoded_int,
    str: decoded_str,
    datetime: decoded_datetime,
    UUID: decoded_uuid,
    type(None): decoded_none,
}


def decoded_primitive[PrimitiveT: Primitive](
    encoded_value: str, type_: type[PrimitiveT]
) -> PrimitiveT:
    decoded = _decoding_by_type[type_]

    return decoded(encoded_value)
