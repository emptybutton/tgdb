from collections.abc import Sequence

from pydantic import BaseModel
from pydantic_core import CoreSchema, core_schema

from tgdb.entities.relation.relation import Relation


class RelationListSchema(BaseModel):
    relations: Sequence[Relation]
