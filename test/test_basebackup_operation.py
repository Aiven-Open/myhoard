# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from . import build_statsd_client, MySQLConfig, restart_mysql
from distutils.version import LooseVersion  # pylint:disable=deprecated-module
from myhoard.basebackup_operation import BasebackupOperation
from typing import IO
from unittest import SkipTest

import myhoard.util as myhoard_util
import os
import pytest

pytestmark = [pytest.mark.unittest, pytest.mark.all]


@pytest.mark.parametrize("extra_uuid", [None, "daf0a972-acd8-44b4-941e-42cbbb43a593"])
def test_basic_backup(mysql_master, extra_uuid):
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        for db_index in range(15):
            cursor.execute(f"CREATE DATABASE test{db_index}")
            cursor.execute(f"CREATE TABLE test{db_index}.foo{db_index} (id integer primary key)")
            for value in range(15):
                cursor.execute(f"INSERT INTO test{db_index}.foo{db_index} (id) VALUES ({value})")
        cursor.execute("COMMIT")
        # Insert second source_uuid into gtid_executed to test that this is parsed correctly
        if extra_uuid:
            cursor.execute(
                "INSERT INTO mysql.gtid_executed (source_uuid, interval_start, interval_end) "
                f"VALUES ('{extra_uuid}', 1, 1)"
            )
            cursor.execute("COMMIT")

    if extra_uuid:
        restart_mysql(mysql_master)

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("SHOW MASTER STATUS")
        master_status = cursor.fetchone()

    # Executed_Gtid_Set has linefeeds in it but XtraBackup (8.0) strips those away, do the same here
    master_status["Executed_Gtid_Set"] = master_status["Executed_Gtid_Set"].replace("\n", "")

    backed_up_files = set()

    # pylint: disable=unused-argument
    def progress_callback(
        *,
        estimated_progress,
        estimated_total_bytes,
        last_file_name,
        last_file_size,
        processed_original_bytes,
    ):
        if last_file_name:
            backed_up_files.add(last_file_name)

    bytes_read = [0]

    def stream_handler(stream):
        while True:
            data = stream.read(10 * 1024)
            if not data:
                break
            bytes_read[0] += len(data)

    encryption_key = os.urandom(24)
    op = BasebackupOperation(
        encryption_algorithm="AES256",
        encryption_key=encryption_key,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        progress_callback=progress_callback,
        stats=build_statsd_client(),
        stream_handler=stream_handler,
        temp_dir=mysql_master.base_dir,
    )
    op.create_backup()

    for db_index in range(15):
        assert f"./test{db_index}/foo{db_index}.ibd" in backed_up_files, f"Couldn't find index {db_index}"

    assert op.binlog_info["gtid"] == master_status["Executed_Gtid_Set"]

    # Taking basebackup might flush binary logs
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("SHOW MASTER STATUS")
        master_status = cursor.fetchone()

    assert op.binlog_info["file_name"] == master_status["File"]
    assert op.binlog_info["file_position"] == master_status["Position"]

    # Even almost empty backup is a few megs due to standard files that are always included
    assert bytes_read[0] > 2 * 1024 * 1024


def test_stream_handler_error_is_propagated(mysql_master):
    def stream_handler(_stream):
        raise ValueError("This is test error")

    encryption_key = os.urandom(24)
    op = BasebackupOperation(
        encryption_algorithm="AES256",
        encryption_key=encryption_key,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        stats=build_statsd_client(),
        stream_handler=stream_handler,
        temp_dir=mysql_master.base_dir,
    )
    with pytest.raises(ValueError, match="^This is test error$"):
        op.create_backup()


def test_fails_on_invalid_params(mysql_master):
    def stream_handler(_stream):
        pass

    op = BasebackupOperation(
        encryption_algorithm="nosuchalgo",
        encryption_key=os.urandom(24),
        mysql_client_params={
            "host": "127.0.0.1",
        },
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        stats=build_statsd_client(),
        stream_handler=stream_handler,
        temp_dir=mysql_master.base_dir,
    )
    # we're opening a connection to check the version on mysql >= 8.0.29 below we're failing with the first message
    with pytest.raises(Exception, match=r"(^xtrabackup failed with code 13$|^mysql_cursor\(\) missing 3 required keyword)"):
        op.create_backup()


def test_backup_with_non_optimized_tables(mysql_master: MySQLConfig) -> None:
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        version = myhoard_util.get_mysql_version(cursor)
        if LooseVersion(version) < LooseVersion("8.0.29"):
            raise SkipTest("DB version doesn't need OPTIMIZE TABLE")

        def create_test_db(*, db_name: str, table_name: str, add_pk: bool) -> None:
            cursor.execute(f"CREATE DATABASE {db_name}")

            if add_pk:
                id_column_type = "integer primary key"
            else:
                id_column_type = "integer"

            cursor.execute(f"CREATE TABLE {db_name}.{table_name} (id {id_column_type})")
            for value in range(15):
                cursor.execute(f"INSERT INTO {db_name}.{table_name} (id) VALUES ({value})")
            cursor.execute("COMMIT")
            cursor.execute(f"ALTER TABLE {db_name}.{table_name} ADD COLUMN foobar VARCHAR(15)")
            cursor.execute("COMMIT")

        for db_index in range(15):
            create_test_db(db_name=f"test{db_index}", table_name=f"foo{db_index}", add_pk=db_index % 2 == 0)

        create_test_db(db_name="`söme/thing'; weird`", table_name="`table with space`", add_pk=True)

    def stream_handler(stream: IO) -> None:
        while True:
            if not stream.read(10 * 1024):
                break

    encryption_key = os.urandom(24)
    op = BasebackupOperation(
        encryption_algorithm="AES256",
        encryption_key=encryption_key,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        optimize_tables_before_backup=True,
        progress_callback=None,
        stats=build_statsd_client(),
        stream_handler=stream_handler,
        temp_dir=mysql_master.base_dir,
    )
    op.create_backup()
