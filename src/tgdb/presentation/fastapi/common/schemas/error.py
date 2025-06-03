from typing import Literal

from pydantic import BaseModel


class NoRelationSchema(BaseModel):
    """
    Relation was not created.
    """

    type: Literal["noRelation"] = "noRelation"


class NotUniqueRelationNumberSchema(BaseModel):
    type: Literal["notUniqueRelationNumber"] = "notUniqueRelationNumber"
