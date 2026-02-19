# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from . import build_statsd_client, wait_for_port
from .helpers.version import xtrabackup_version_to_string
from myhoard.basebackup_operation import BasebackupOperation
from myhoard.basebackup_restore_operation import BasebackupRestoreOperation
from unittest.mock import patch

import myhoard.util as myhoard_util
import os
import pytest
import shutil
import subprocess
import tempfile

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def test_get_xtrabackup_cmd():
    op_kwargs = {
        "encryption_algorithm": "AES256",
        "encryption_key": "123",
        "free_memory_percentage": 80,
        "mysql_config_file_name": "/etc/mysql/mysql.conf",
        "mysql_data_directory": "/usr/lib/mysql/",
        "stats": build_statsd_client(),
        "stream_handler": None,
        "target_dir": "",
        "temp_dir": "",
    }
    op = BasebackupRestoreOperation(**op_kwargs)
    cmd = op.get_xtrabackup_cmd()
    assert cmd == "xtrabackup"
    xtrabackup_path = shutil.which("xtrabackup")
    xtrabackup_dir = os.path.dirname(xtrabackup_path)
    xtrabackup_version = myhoard_util.get_xtrabackup_version()
    with patch.dict(os.environ, {"PXB_EXTRA_BIN_PATHS": xtrabackup_dir}):
        cmd = BasebackupRestoreOperation(
            **op_kwargs, backup_tool_version=xtrabackup_version_to_string(xtrabackup_version)
        ).get_xtrabackup_cmd()
        assert cmd == xtrabackup_path
        cmd = BasebackupRestoreOperation(**op_kwargs, backup_tool_version="8.0.0").get_xtrabackup_cmd()
        assert cmd == "xtrabackup"


def test_basic_restore(mysql_master, mysql_empty):
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        for db_index in range(15):
            cursor.execute(f"CREATE DATABASE test{db_index}")
            cursor.execute(f"CREATE TABLE test{db_index}.foo{db_index} (id integer primary key)")
            for value in range(15):
                cursor.execute(f"INSERT INTO test{db_index}.foo{db_index} (id) VALUES ({value})")
        cursor.execute("FLUSH LOGS")
        cursor.execute(mysql_master.show_binary_logs_status_cmd)
        old_master_status = cursor.fetchone()

    encryption_key = os.urandom(24)

    with tempfile.NamedTemporaryFile() as backup_file:

        def output_stream_handler(stream):
            shutil.copyfileobj(stream, backup_file)

        backup_op = BasebackupOperation(
            encryption_algorithm="AES256",
            encryption_key=encryption_key,
            mysql_client_params=mysql_master.connect_options,
            mysql_config_file_name=mysql_master.config_name,
            mysql_data_directory=mysql_master.config_options.datadir,
            stats=build_statsd_client(),
            stream_handler=output_stream_handler,
            temp_dir=mysql_master.base_dir,
        )
        backup_op.create_backup()

        backup_file.seek(0)

        def input_stream_handler(stream):
            shutil.copyfileobj(backup_file, stream)
            stream.close()

        with tempfile.TemporaryDirectory(dir=mysql_empty.base_dir, prefix="myhoard_target_") as temp_target_dir:
            restore_op = BasebackupRestoreOperation(
                encryption_algorithm="AES256",
                encryption_key=encryption_key,
                free_memory_percentage=80,
                mysql_config_file_name=mysql_empty.config_name,
                mysql_data_directory=mysql_empty.config_options.datadir,
                stats=build_statsd_client(),
                stream_handler=input_stream_handler,
                target_dir=temp_target_dir,
                temp_dir=mysql_empty.base_dir,
                backup_tool_version=xtrabackup_version_to_string(myhoard_util.get_xtrabackup_version()),
            )
            restore_op.prepare_backup()
            restore_op.restore_backup()

        assert restore_op.number_of_files >= backup_op.number_of_files

    mysql_empty.proc = subprocess.Popen(mysql_empty.startup_command)  # pylint: disable=consider-using-with
    wait_for_port(mysql_empty.port)

    with myhoard_util.mysql_cursor(
        password=mysql_master.password,
        port=mysql_empty.port,
        user=mysql_master.user,
    ) as cursor:
        for db_index in range(15):
            cursor.execute(f"SELECT id FROM test{db_index}.foo{db_index}")
            results = cursor.fetchall()
            assert sorted(result["id"] for result in results) == sorted(range(15))
        cursor.execute(mysql_master.show_binary_logs_status_cmd)
        new_master_status = cursor.fetchone()
        assert old_master_status["Executed_Gtid_Set"] == new_master_status["Executed_Gtid_Set"]


def test_incremental_backup_restore(mysql_master, mysql_empty) -> None:
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        for db_index in range(5):
            cursor.execute(f"CREATE DATABASE test{db_index}")
            cursor.execute(f"CREATE TABLE test{db_index}.foo{db_index} (id integer primary key)")
            for value in range(10):
                cursor.execute(f"INSERT INTO test{db_index}.foo{db_index} (id) VALUES ({value})")
        cursor.execute("FLUSH LOGS")

    encryption_key = os.urandom(24)

    with tempfile.NamedTemporaryFile() as backup_file1, tempfile.NamedTemporaryFile() as backup_file2:

        def build_stream_handler(backup_file):
            def output_stream_handler(stream):
                shutil.copyfileobj(stream, backup_file)

            return output_stream_handler

        backup_op = BasebackupOperation(
            encryption_algorithm="AES256",
            encryption_key=encryption_key,
            mysql_client_params=mysql_master.connect_options,
            mysql_config_file_name=mysql_master.config_name,
            mysql_data_directory=mysql_master.config_options.datadir,
            stats=build_statsd_client(),
            stream_handler=build_stream_handler(backup_file1),
            temp_dir=mysql_empty.base_dir,
        )
        backup_op.create_backup()

        with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
            for db_index in range(5, 10):
                cursor.execute(f"CREATE DATABASE test{db_index}")
                cursor.execute(f"CREATE TABLE test{db_index}.foo{db_index} (id integer primary key)")
                for value in range(10):
                    cursor.execute(f"INSERT INTO test{db_index}.foo{db_index} (id) VALUES ({value})")
            cursor.execute("FLUSH LOGS")
            cursor.execute(mysql_master.show_binary_logs_status_cmd)
            old_master_status = cursor.fetchone()
            assert old_master_status

        backup_op_inc = BasebackupOperation(
            encryption_algorithm="AES256",
            encryption_key=encryption_key,
            mysql_client_params=mysql_master.connect_options,
            mysql_config_file_name=mysql_master.config_name,
            mysql_data_directory=mysql_master.config_options.datadir,
            stats=build_statsd_client(),
            stream_handler=build_stream_handler(backup_file2),
            temp_dir=mysql_empty.base_dir,
            incremental_since_checkpoint=backup_op.checkpoints_file_content,
        )
        backup_op_inc.create_backup()

        def build_input_stream_handler(backup_file):
            backup_file.seek(0)

            def input_stream_handler(stream):
                shutil.copyfileobj(backup_file, stream)
                stream.close()

            return input_stream_handler

        with tempfile.TemporaryDirectory(dir=mysql_empty.base_dir, prefix="myhoard_target_") as temp_target_dir:
            restore_op = BasebackupRestoreOperation(
                encryption_algorithm="AES256",
                encryption_key=encryption_key,
                free_memory_percentage=80,
                mysql_config_file_name=mysql_empty.config_name,
                mysql_data_directory=mysql_empty.config_options.datadir,
                stats=build_statsd_client(),
                stream_handler=build_input_stream_handler(backup_file1),
                target_dir=temp_target_dir,
                temp_dir=mysql_empty.base_dir,
            )
            restore_op.prepare_backup(
                incremental=False, apply_log_only=True, checkpoints_file_content=backup_op.checkpoints_file_content
            )
            restore_op_inc = BasebackupRestoreOperation(
                encryption_algorithm="AES256",
                encryption_key=encryption_key,
                free_memory_percentage=80,
                mysql_config_file_name=mysql_empty.config_name,
                mysql_data_directory=mysql_empty.config_options.datadir,
                stats=build_statsd_client(),
                stream_handler=build_input_stream_handler(backup_file2),
                target_dir=temp_target_dir,
                temp_dir=mysql_empty.base_dir,
            )
            restore_op_inc.prepare_backup(
                incremental=True, apply_log_only=False, checkpoints_file_content=backup_op_inc.checkpoints_file_content
            )
            restore_op_inc.restore_backup()

        assert restore_op_inc.number_of_files >= backup_op.number_of_files

    mysql_empty.proc = subprocess.Popen(mysql_empty.startup_command)  # pylint: disable=consider-using-with
    wait_for_port(mysql_empty.port)

    with myhoard_util.mysql_cursor(
        password=mysql_master.password,
        port=mysql_empty.port,
        user=mysql_master.user,
    ) as cursor:
        for db_index in range(10):
            cursor.execute(f"SELECT id FROM test{db_index}.foo{db_index}")
            results = cursor.fetchall()
            assert sorted(result["id"] for result in results) == sorted(range(10))
        cursor.execute(mysql_master.show_binary_logs_status_cmd)
        new_master_status = cursor.fetchone()
        assert new_master_status
        assert old_master_status["Executed_Gtid_Set"] == new_master_status["Executed_Gtid_Set"]
