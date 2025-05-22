from enum import Enum


class Tag(Enum):
    transaction = "User"
    monitoring = "Transaction"


tags_metadata = [
    {
        "name": Tag.monitoring.value,
        "description": "Monitoring endpoints.",
    },
    {
        "name": Tag.transaction.value,
        "description": "Transaction endpoints.",
    },
]
