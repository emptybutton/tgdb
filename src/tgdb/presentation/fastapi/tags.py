from enum import Enum


class Tag(Enum):
    monitoring = "Monitoring"
    transaction = "Transaction"
    relation = "Relation"


tags_metadata = [
    {
        "name": Tag.monitoring.value,
        "description": "Monitoring endpoints.",
    },
    {
        "name": Tag.transaction.value,
        "description": "Transaction endpoints.",
    },
    {
        "name": Tag.relation.value,
        "description": "Relation endpoints.",
    },
]
