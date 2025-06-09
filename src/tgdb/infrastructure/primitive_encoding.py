from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast
from uuid import UUID


type Primitive = None | bool | int | str | datetime | UUID  # noqa: RUF036


_decoded_primitive_type_map = dict[str, type[Primitive]](
    n=type(None),
    b=bool,
    i=int,
    s=str,
    d=datetime,
    u=UUID,
)
_encoded_primitive_type_map = dict(
    zip(
        _decoded_primitive_type_map.values(),
        _decoded_primitive_type_map.keys(),
        strict=True,
    ),
)


@dataclass
class ReversibleTranslationTable:
    _map: Mapping[int, int | None]
    _reversed_map: Mapping[int, int] = field(init=False)

    def __post_init__(self) -> None:
        self._reversed_map = {
            value: key for key, value in self._map.items() if value is not None
        }

    def map(self) -> Mapping[int, int | None]:
        return self._map

    def reversed_map(self) -> Mapping[int, int]:
        return self._reversed_map


empty_table = ReversibleTranslationTable(dict())


def encoded_primitive_without_type(
    primitive: Primitive,
    table: ReversibleTranslationTable,
) -> str:
    match primitive:
        case bool():
            return encoded_bool(primitive)
        case int():
            return encoded_int(primitive)
        case str():
            return encoded_str(primitive, table)
        case datetime():
            return encoded_datetime(primitive)
        case None:
            return encoded_none()
        case UUID():
            return encoded_uuid(primitive)


def encoded_int(int_: int) -> str:
    return str(int_)


def encoded_bool(bool_: bool) -> str:  # noqa: FBT001
    return str(int(bool_))


def encoded_str(str_: str, table: ReversibleTranslationTable) -> str:
    return str_.translate(table.map())


def encoded_datetime(datetime: datetime) -> str:
    return datetime.isoformat()


def encoded_none() -> str:
    return ""


def encoded_uuid(uuid: UUID) -> str:
    return uuid.hex


def encoded_primitive_with_type(
    primitive: Primitive,
    table: ReversibleTranslationTable,
) -> str:
    body = encoded_primitive_without_type(primitive, table)
    header = _encoded_primitive_type_map[type(primitive)]

    return f"{header}{body}"


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
    return encoded_value


def decoded_datetime(encoded_value: str) -> datetime:
    return datetime.fromisoformat(encoded_value)


def decoded_uuid(encoded_value: str) -> UUID:
    return UUID(hex=encoded_value)


def decoded_none(encoded_value: str) -> None:
    if encoded_value:
        raise ValueError(encoded_value)


_decoded_body_func_by_type: dict[type[Any], Callable[[str], Primitive]] = {
    bool: decoded_bool,
    int: decoded_int,
    str: decoded_str,
    datetime: decoded_datetime,
    UUID: decoded_uuid,
    type(None): decoded_none,
}


def decoded_primitive_with_type(
    encoded_value: str,
    table: ReversibleTranslationTable,
) -> Primitive:
    encoded_value = encoded_value.translate(table.reversed_map())
    header = encoded_value[0]
    body = encoded_value[1:]

    decoded_primitive_type = _decoded_primitive_type_map[header]
    decoded_body_func = _decoded_body_func_by_type[decoded_primitive_type]

    return decoded_body_func(body)


def decoded_primitive_without_type[PrimitiveT: Primitive](
    encoded_value: str,
    table: ReversibleTranslationTable,
    type_: type[PrimitiveT],
) -> PrimitiveT:
    encoded_value = encoded_value.translate(table.reversed_map())
    decoded_body = cast(
        Callable[[str], PrimitiveT],
        _decoded_body_func_by_type[type_],
    )

    return decoded_body(encoded_value)
