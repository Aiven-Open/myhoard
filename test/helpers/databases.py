# Copyright (c) 2023 Aiven, Helsinki, Finland. https://aiven.io/
from myhoard.util import mysql_cursor
from test import MySQLConfig
from test.helpers.loggers import get_logger_name
from typing import cast, Final, TypedDict

import logging
import time


class SizeDict(TypedDict):
    size: int


def populate_table(mysql_config: MySQLConfig, table_name: str, batches: int = 1) -> None:
    """Populate database with a lot of data, using a single transaction.

    Args:
        mysql_config: Configuration for connecting to MySQL.
        table_name: Name of the table to populate (will be created if it does not exist).
        batches: Number of batches to use. Each batch is 64 MB.
    """
    logger = logging.getLogger(get_logger_name())

    ONE_MB: Final[int] = 2**20
    MB_PER_BATCH: Final[int] = 64

    # Use a higher timeout, +1 minute per 4 batches
    options = mysql_config.connect_options
    if batches > 3:
        orig_timeout = mysql_config.connect_options["timeout"]
        options = mysql_config.connect_options | {"timeout": orig_timeout + batches // 4 * 60}

    # Use a higher timeout
    with mysql_cursor(**options) as cursor:
        t0 = time.monotonic_ns()

        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, b LONGBLOB);")
        cursor.execute("DROP PROCEDURE IF EXISTS generate_data;")
        cursor.execute(
            f"""
        CREATE PROCEDURE generate_data()
        BEGIN
             DECLARE i INT DEFAULT 0;
                WHILE i < {batches} DO
                    INSERT INTO {table_name} (b) VALUES (REPEAT('x', {MB_PER_BATCH * ONE_MB}));
                    SET i = i + 1;
                END WHILE;
        END
        """
        )
        cursor.execute("CALL generate_data();")
        cursor.execute("COMMIT")
        cursor.execute("FLUSH BINARY LOGS")

        t1 = time.monotonic_ns()

        logger.info(
            "Populating table %s with %i MB took %f sec.", table_name, (batches * MB_PER_BATCH), (t1 - t0) / 1_000_000_000
        )


def get_table_size(mysql_config: MySQLConfig, table_name: str) -> int:
    """Get size of table (data + index) in bytes."""
    with mysql_cursor(**mysql_config.connect_options) as cursor:
        cursor.execute(
            f"""
            SELECT TABLE_NAME AS `Table`,
                (DATA_LENGTH + INDEX_LENGTH) AS `size`
            FROM information_schema.TABLES
            WHERE TABLE_NAME = '{table_name}';
        """
        )

        return cast(SizeDict, cursor.fetchone())["size"]
