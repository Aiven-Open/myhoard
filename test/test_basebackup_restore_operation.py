# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import os
import shutil
import subprocess
import tempfile

import pytest

import myhoard.util as myhoard_util
from myhoard.basebackup_operation import BasebackupOperation
from myhoard.basebackup_restore_operation import BasebackupRestoreOperation

from . import build_statsd_client, wait_for_port

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def test_basic_restore(mysql_master, mysql_empty):
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        for db_index in range(15):
            cursor.execute(f"CREATE DATABASE test{db_index}")
            cursor.execute(f"CREATE TABLE test{db_index}.foo{db_index} (id integer primary key)")
            for value in range(15):
                cursor.execute(f"INSERT INTO test{db_index}.foo{db_index} (id) VALUES ({value})")
        cursor.execute("FLUSH LOGS")
        cursor.execute("SHOW MASTER STATUS")
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

        restore_op = BasebackupRestoreOperation(
            encryption_algorithm="AES256",
            encryption_key=encryption_key,
            mysql_config_file_name=mysql_empty.config_name,
            mysql_data_directory=mysql_empty.config_options.datadir,
            stats=build_statsd_client(),
            stream_handler=input_stream_handler,
            temp_dir=mysql_empty.base_dir,
        )
        restore_op.restore_backup()

        assert restore_op.number_of_files >= backup_op.number_of_files

    mysql_empty.proc = subprocess.Popen(mysql_empty.startup_command)
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
        cursor.execute("SHOW MASTER STATUS")
        new_master_status = cursor.fetchone()
        assert old_master_status["Executed_Gtid_Set"] == new_master_status["Executed_Gtid_Set"]
