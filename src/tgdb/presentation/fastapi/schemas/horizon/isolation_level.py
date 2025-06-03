from enum import StrEnum
from typing import Literal

from tgdb.entities.horizon.transaction import IsolationLevel


type _EncodedIsolationLevel = Literal[
    "serializableReadAndWrite", "nonSerializableRead"
]


class IsolationLevelSchema(StrEnum):
    serializable_read_and_write = "serializableReadAndWrite"
    non_serializable_read = "nonSerializableRead"

    def decoded(self) -> IsolationLevel:
        match self:
            case IsolationLevelSchema.serializable_read_and_write:
                return IsolationLevel.serializable_read_and_write
            case IsolationLevelSchema.non_serializable_read:
                return IsolationLevel.non_serializable_read

    @classmethod
    def of(cls, level: IsolationLevel) -> "IsolationLevelSchema":
        match level:
            case IsolationLevel.serializable_read_and_write:
                return IsolationLevelSchema.serializable_read_and_write
            case IsolationLevel.non_serializable_read:
                return IsolationLevelSchema.non_serializable_read
