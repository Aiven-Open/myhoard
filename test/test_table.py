# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from myhoard.table import escape_identifier, Table


def test_create_table_from_row() -> None:
    table = Table.from_row(
        {
            "TABLE_SCHEMA": "schema",
            "TABLE_NAME": "name",
            "TABLE_ROWS": 123456,
            "AVG_ROW_LENGTH": 100,
        }
    )
    assert table.table_schema == "schema"
    assert table.table_name == "name"
    assert table.table_rows == 123456
    assert table.avg_row_length == 100


def test_table_estimated_size_bytes() -> None:
    table = Table(table_schema="schema", table_name="name", table_rows=10, avg_row_length=20)
    assert table.estimated_size_bytes() == 200


def test_table_escaped_designator() -> None:
    table = Table(table_schema="bad`sch``ema", table_name="bad`name", table_rows=10, avg_row_length=20)
    assert table.escaped_designator() == "`bad``sch````ema`.`bad``name`"


def test_escape_identifier() -> None:
    assert escape_identifier("name") == "`name`"
    assert escape_identifier("na`me") == "`na``me`"
    assert escape_identifier("na``me") == "`na````me`"
