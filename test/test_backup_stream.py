# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import json
import os

import pytest
from pghoard.rohmu.object_storage.local import LocalTransfer

import myhoard.util as myhoard_util
from myhoard.backup_stream import BackupStream
from myhoard.binlog_scanner import BinlogScanner
from myhoard.controller import Controller

from . import (MySQLConfig, build_statsd_client, generate_rsa_key_pair, wait_for_condition)

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def test_backup_stream(session_tmpdir, mysql_master):
    _run_backup_stream_test(session_tmpdir, mysql_master, BackupStream)


def test_backup_stream_with_s3_emulation(session_tmpdir, mysql_master):
    class PatchedBackupStream(BackupStream):
        def _should_list_with_metadata(self, *, next_index):  # pylint: disable=unused-argument
            return False

    _run_backup_stream_test(session_tmpdir, mysql_master, PatchedBackupStream)


def _run_backup_stream_test(session_tmpdir, mysql_master: MySQLConfig, backup_stream_class):
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("CREATE DATABASE db1")
        cursor.execute("USE db1")
        cursor.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, data TEXT)")
        cursor.execute("COMMIT")

    BackupStream.ITERATION_SLEEP = 0.1
    BackupStream.REMOTE_POLL_INTERVAL = 0.1

    backup_target_location = session_tmpdir().strpath
    state_file_name = os.path.join(session_tmpdir().strpath, "backup_stream.json")
    private_key_pem, public_key_pem = generate_rsa_key_pair()  # pylint: disable=unused-variable
    bs = backup_stream_class(
        backup_reason=BackupStream.BackupReason.requested,
        compression={
            "algorithm": "lzma",
            "level": 1,
        },
        file_storage_setup_fn=lambda: LocalTransfer(backup_target_location),
        mode=BackupStream.Mode.active,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        normalized_backup_time="2019-02-25T08:20",
        rsa_public_key_pem=public_key_pem,
        server_id=mysql_master.server_id,
        site="default",
        state_file=state_file_name,
        stats=build_statsd_client(),
        temp_dir=mysql_master.base_dir,
    )

    scanner = BinlogScanner(
        binlog_prefix=mysql_master.config_options.binlog_file_prefix,
        server_id=mysql_master.server_id,
        state_file=os.path.join(session_tmpdir().strpath, "scanner_state.json"),
        stats=build_statsd_client(),
    )
    bs.add_binlogs(scanner.scan_new(None))

    _private_key_pem, public_key_pem = generate_rsa_key_pair()
    bs_observer = backup_stream_class(
        backup_reason=None,
        file_storage_setup_fn=lambda: LocalTransfer(backup_target_location),
        mode=BackupStream.Mode.observe,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        normalized_backup_time="2019-02-25T08:20",
        rsa_public_key_pem=public_key_pem,
        server_id=mysql_master.server_id,
        site="default",
        state_file=os.path.join(session_tmpdir().strpath, "backup_stream_observer.json"),
        stats=build_statsd_client(),
        stream_id=bs.stream_id,
        temp_dir=mysql_master.base_dir,
    )

    with bs_observer.running():
        with bs.running():
            wait_for_condition(bs.is_streaming_binlogs, timeout=15)
            wait_for_condition(lambda: bs_observer.state["last_remote_state_check"], timeout=10)

            with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
                cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
                gtid_executed = myhoard_util.parse_gtid_range_string(cursor.fetchone()["gtid_executed"])
                assert bs_observer.state["remote_gtid_executed"] == gtid_executed
                cursor.execute("INSERT INTO db1.t1 (id, data) VALUES (1, 'abcdefg')")
                cursor.execute("COMMIT")
                cursor.execute("FLUSH BINARY LOGS")

            new_binlogs = scanner.scan_new(None)
            assert new_binlogs
            bs.add_binlogs(new_binlogs)
            wait_for_condition(lambda: not bs.state["pending_binlogs"])
            bs.mark_as_completed()
            wait_for_condition(lambda: bs.state["active_details"]["phase"] == BackupStream.ActivePhase.binlog)

        assert bs.is_binlog_safe_to_delete(new_binlogs[0])
        assert bs.is_log_backed_up(log_index=new_binlogs[0]["local_index"])

        # remote_gtid_executed will be updated once the stream notices the new binlog that was uploaded above
        wait_for_condition(lambda: bs_observer.state["remote_gtid_executed"] != gtid_executed)
        # Is safe to delete because all GTIDs in this binlog have been backed up (but not by this stream)
        assert bs_observer.is_binlog_safe_to_delete(new_binlogs[0])
        # This stream isn't in active mode so is_log_backed_up will return false for any input
        assert not bs_observer.is_log_backed_up(log_index=new_binlogs[0]["local_index"])
        # Check the compression algorithm for binlogs is set as expected
        assert bs_observer.remote_binlogs[0]["compression_algorithm"] == "lzma"

        assert bs.state["basebackup_errors"] == 0
        assert bs.state["remote_read_errors"] == 0
        assert bs.state["remote_write_errors"] == 0
        with open(state_file_name) as f:
            assert bs.state == json.load(f)

        backup_sites = {
            "default": {
                "object_storage": {
                    "directory": backup_target_location,
                    "storage_type": "local",
                }
            }
        }
        backups = Controller.get_backup_list(backup_sites)
        assert len(backups) == 1
        backup = backups[0]
        assert not backup["closed_at"]
        assert backup["completed_at"]
        assert backup["stream_id"]
        assert backup["resumable"]
        assert backup["site"] == "default"

        with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
            cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
            gtid_executed = myhoard_util.parse_gtid_range_string(cursor.fetchone()["gtid_executed"])
            assert bs_observer.state["remote_gtid_executed"] == gtid_executed
            cursor.execute("INSERT INTO db1.t1 (id, data) VALUES (2, 'hijkl')")
            cursor.execute("COMMIT")
            cursor.execute("FLUSH BINARY LOGS")

        new_binlogs = scanner.scan_new(None)
        assert new_binlogs
        bs.add_binlogs(new_binlogs)
        assert bs.state["pending_binlogs"]
        assert not bs.is_binlog_safe_to_delete(new_binlogs[0])
        assert not bs.is_log_backed_up(log_index=new_binlogs[0]["local_index"])
        assert not bs_observer.is_binlog_safe_to_delete(new_binlogs[0])
        assert not bs_observer.is_log_backed_up(log_index=new_binlogs[0]["local_index"])

    bs.state_manager.update_state(initial_latest_complete_binlog_index=new_binlogs[0]["local_index"])
    assert bs.is_binlog_safe_to_delete(new_binlogs[0])
