from abc import ABC, abstractmethod

from tgdb.entities.numeration.number import Number
from tgdb.entities.relation.relation import Relation


class NotUniqueRelationNumberError(Exception): ...


class Relations(ABC):
    @abstractmethod
    async def relation(self, relation_number: Number) -> Relation | None: ...

    @abstractmethod
    async def add(self, relation: Relation) -> None:
        """
        :raises tgdb.application.ports.relations.NotUniqueRelationNumberError:
        """
