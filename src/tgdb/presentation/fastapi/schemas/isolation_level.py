from typing import Annotated, Literal

from pydantic import AfterValidator, PlainSerializer

from tgdb.entities.horizon.transaction import IsolationLevel


type _EncodedIsolationLevel = Literal[
    "serializableReadAndWrite", "nonSerializableRead"
]


def _validated_isolation_level(str: str) -> IsolationLevel:
    match str:
        case "serializableReadAndWrite":
            return IsolationLevel.serializable_read_and_write
        case "nonSerializableRead":
            return IsolationLevel.non_serializable_read
        case _:
            raise ValueError("unknown level name")  # noqa: TRY003


def _serialized_isolation_level(
    level: IsolationLevel
) -> _EncodedIsolationLevel:
    match level:
        case IsolationLevel.serializable_read_and_write:
            return "serializableReadAndWrite"
        case IsolationLevel.non_serializable_read:
            return "nonSerializableRead"


EncodableIsolationLevel = Annotated[
    IsolationLevel,
    PlainSerializer(_serialized_isolation_level),
    AfterValidator(_validated_isolation_level),
]
