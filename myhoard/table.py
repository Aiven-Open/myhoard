#  Copyright (c) 23 Aiven, Helsinki, Finland. https://aiven.io/
from typing import Any, Mapping, Tuple

import dataclasses


@dataclasses.dataclass(frozen=True)
class Table:
    table_schema: str
    table_name: str
    table_rows: int
    avg_row_length: int

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "Table":
        return Table(
            table_schema=row["TABLE_SCHEMA"],
            table_name=row["TABLE_NAME"],
            table_rows=row["TABLE_ROWS"],
            avg_row_length=row["AVG_ROW_LENGTH"],
        )

    def sort_key(self) -> Tuple[str, str]:
        return self.table_schema, self.table_name

    def estimated_size_bytes(self) -> int:
        return self.table_rows * self.avg_row_length

    def escaped_designator(self) -> str:
        escaped_table_schema = escape_identifier(self.table_schema)
        escaped_table_name = escape_identifier(self.table_name)
        return f"{escaped_table_schema}.{escaped_table_name}"


def escape_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"
