from abc import ABC, abstractmethod
from dataclasses import dataclass

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation


@dataclass(frozen=True)
class NotUniqueRelationNumberError(Exception): ...


@dataclass(frozen=True)
class NoRelationError(Exception): ...


class Relations(ABC):
    @abstractmethod
    async def relation(self, relation_number: Number) -> Relation:
        """
        :raises tgdb.application.common.ports.relations.NoRelationError:
        """

    @abstractmethod
    async def add(self, relation: Relation) -> None:
        """
        :raises tgdb.application.common.ports.relations.NotUniqueRelationNumberError:
        """  # noqa: E501
