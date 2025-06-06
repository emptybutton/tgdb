from enum import StrEnum
from typing import Literal

from tgdb.entities.horizon.transaction import IsolationLevel


type _EncodedIsolationLevel = Literal["serializable", "readUncommited"]


class IsolationLevelSchema(StrEnum):
    serializable = "serializable"
    read_uncommited = "readUncommited"

    def decoded(self) -> IsolationLevel:
        match self:
            case IsolationLevelSchema.serializable:
                return IsolationLevel.serializable
            case IsolationLevelSchema.read_uncommited:
                return IsolationLevel.read_uncommited

    @classmethod
    def of(cls, level: IsolationLevel) -> "IsolationLevelSchema":
        match level:
            case IsolationLevel.serializable:
                return IsolationLevelSchema.serializable
            case IsolationLevel.read_uncommited:
                return IsolationLevelSchema.read_uncommited
