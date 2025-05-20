from tgdb.entities.row import RowAttribute
from tgdb.infrastructure.primitive_encoding import encoded_primitive


type Query = str


def row_with_id_query(row_id: RowAttribute) -> Query:
    return f"#0={encoded_primitive(row_id)}"
