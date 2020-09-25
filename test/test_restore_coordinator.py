# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import os
import time

import pytest
from pghoard.rohmu.object_storage.local import LocalTransfer

import myhoard.util as myhoard_util
from myhoard.backup_stream import BackupStream
from myhoard.binlog_scanner import BinlogScanner
from myhoard.restore_coordinator import RestoreCoordinator

from . import (DataGenerator, build_statsd_client, generate_rsa_key_pair, restart_mysql, while_asserts)

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def test_restore_coordinator(session_tmpdir, mysql_master, mysql_empty):
    _restore_coordinator_sequence(session_tmpdir, mysql_master, mysql_empty, pitr=False)


def test_restore_coordinator_pitr(session_tmpdir, mysql_master, mysql_empty):
    _restore_coordinator_sequence(session_tmpdir, mysql_master, mysql_empty, pitr=True)


def _restore_coordinator_sequence(session_tmpdir, mysql_master, mysql_empty, *, pitr):
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("CREATE DATABASE db1")
        cursor.execute("USE db1")
        cursor.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, data TEXT)")
        cursor.execute("COMMIT")

    private_key_pem, public_key_pem = generate_rsa_key_pair()
    backup_target_location = session_tmpdir().strpath
    state_file_name1 = os.path.join(session_tmpdir().strpath, "backup_stream1.json")
    state_file_name2 = os.path.join(session_tmpdir().strpath, "backup_stream2.json")
    bs1 = BackupStream(
        backup_reason=BackupStream.BackupReason.requested,
        file_storage_setup_fn=lambda: LocalTransfer(backup_target_location),
        mode=BackupStream.Mode.active,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        normalized_backup_time="2019-02-25T08:20",
        rsa_public_key_pem=public_key_pem,
        server_id=mysql_master.server_id,
        site="default",
        state_file=state_file_name1,
        stats=build_statsd_client(),
        temp_dir=mysql_master.base_dir,
    )
    # Use two backup streams to test recovery mode where basebackup is restored from earlier
    # backup and binlogs are applied from several different backups
    bs2 = BackupStream(
        backup_reason=BackupStream.BackupReason.requested,
        file_storage_setup_fn=lambda: LocalTransfer(backup_target_location),
        mode=BackupStream.Mode.active,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        normalized_backup_time="2019-02-26T08:20",
        rsa_public_key_pem=public_key_pem,
        server_id=mysql_master.server_id,
        site="default",
        state_file=state_file_name2,
        stats=build_statsd_client(),
        temp_dir=mysql_master.base_dir,
    )

    data_generator = DataGenerator(
        connect_info=mysql_master.connect_options,
        # Don't make temp tables when we're testing point-in-time recovery because even though
        # those should work fine the current way of priming data makes it difficult to verify
        # which data should've been inserted at given point in time
        make_temp_tables=not pitr,
    )
    data_generator.start()

    # Increasing this number makes the system generate more data, allowing more realistic tests
    # (but unsuitable as regular unit test)
    phase_duration = 0.5
    data_gen_start = time.monotonic()

    # Let is generate some initial data
    time.sleep(phase_duration * 2)

    scanner = BinlogScanner(
        binlog_prefix=mysql_master.config_options.binlog_file_prefix,
        server_id=mysql_master.server_id,
        state_file=os.path.join(session_tmpdir().strpath, "scanner_state.json"),
        stats=build_statsd_client(),
    )
    binlogs = scanner.scan_new(None)
    bs1.add_binlogs(binlogs)
    bs2.add_binlogs(binlogs)

    print(
        int(time.monotonic() - data_gen_start),
        "initial rows:", data_generator.row_count,
        "estimated bytes:", data_generator.estimated_bytes,
        "binlog count:", len(scanner.binlogs),
    )  # yapf: disable

    bs1.start()

    start = time.monotonic()
    while not bs1.is_streaming_binlogs():
        assert time.monotonic() - start < 30
        time.sleep(0.1)
        binlogs = scanner.scan_new(None)
        bs1.add_binlogs(binlogs)
        bs2.add_binlogs(binlogs)

    print(
        int(time.monotonic() - data_gen_start),
        "rows when basebackup 1 finished:", data_generator.row_count,
        "estimated bytes:", data_generator.estimated_bytes,
        "binlog count:", len(scanner.binlogs),
    )  # yapf: disable

    bs2.start()

    while not bs2.is_streaming_binlogs():
        assert time.monotonic() - start < 30
        time.sleep(0.1)
        binlogs = scanner.scan_new(None)
        bs1.add_binlogs(binlogs)
        bs2.add_binlogs(binlogs)

    print(
        int(time.monotonic() - data_gen_start),
        "rows when basebackup 2 finished:", data_generator.row_count,
        "estimated bytes:", data_generator.estimated_bytes,
        "binlog count:", len(scanner.binlogs),
    )  # yapf: disable

    # Wait until bs1 has uploaded all binlogs, then mark that backup as closed and stop it
    start = time.monotonic()
    while bs1.state["pending_binlogs"]:
        assert time.monotonic() - start < 10
        time.sleep(0.1)

    bs1.mark_as_completed()
    bs1.mark_as_closed()

    def stream_is_closed(stream):
        assert stream.state["active_details"]["phase"] == BackupStream.ActivePhase.none

    # Wait for the close operation to be handled before stopping the stream
    while_asserts(lambda: stream_is_closed(bs1))
    bs1.stop()

    start = time.monotonic()
    while time.monotonic() - start < phase_duration:
        time.sleep(0.1)
        bs2.add_binlogs(scanner.scan_new(None))

    pitr_master_status = None
    pitr_row_count = None
    pitr_target_time = None
    if pitr:
        # Pause the data generator for a while
        data_generator.generate_data_event.clear()
        while not data_generator.paused:
            time.sleep(0.1)
        # Times in logs are with one second precision so need to sleep at least one second before and after
        # to make sure we restore exactly the events we want. To avoid any transient failures sleep a bit more
        time.sleep(2)
        pitr_row_count = data_generator.committed_row_count
        with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
            cursor.execute("SHOW MASTER STATUS")
            pitr_master_status = cursor.fetchone()
            print(time.time(), "Master status at PITR target time", pitr_master_status)
        pitr_target_time = int(time.time())
        time.sleep(2)
        data_generator.generate_data_event.set()

    print(
        int(time.monotonic() - data_gen_start),
        "rows when marking complete:", data_generator.row_count,
        "estimated bytes:", data_generator.estimated_bytes,
        "binlog count:", len(scanner.binlogs),
    )  # yapf: disable
    bs2.mark_as_completed()

    start = time.monotonic()
    while time.monotonic() - start < phase_duration:
        time.sleep(0.1)
        bs2.add_binlogs(scanner.scan_new(None))

    data_generator.is_running = False
    data_generator.join()
    # Force binlog flush, earlier data generation might not have flushed binlogs in which case the target
    # entry would not be available for recovery
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("FLUSH BINARY LOGS")

    bs2.add_binlogs(scanner.scan_new(None))

    print(
        int(time.monotonic() - data_gen_start),
        "final rows:", data_generator.row_count,
        "estimated bytes:", data_generator.estimated_bytes,
        "binlog count:", len(scanner.binlogs),
    )  # yapf: disable

    start = time.monotonic()
    while bs2.state["pending_binlogs"]:
        assert time.monotonic() - start < 10
        time.sleep(0.1)

    bs2.stop()

    assert bs2.state["active_details"]["phase"] == BackupStream.ActivePhase.binlog

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("SHOW MASTER STATUS")
        original_master_status = cursor.fetchone()
        print("Original master status", original_master_status)
        cursor.execute("SELECT COUNT(*) AS count FROM db1.t1")
        original_count = cursor.fetchone()["count"]
        print("Number of rows in original master", original_count)

    restored_connect_options = {
        "host": "127.0.0.1",
        "password": mysql_master.password,
        "port": mysql_empty.port,
        "user": mysql_master.user,
    }
    state_file_name = os.path.join(session_tmpdir().strpath, "restore_coordinator.json")
    # Restore basebackup from backup stream 1 but binlogs from both streams. First stream doesn't
    # contain latest state but applying binlogs from second stream should get it fully up-to-date
    rc = RestoreCoordinator(
        binlog_streams=[{
            "site": "default",
            "stream_id": bs1.state["stream_id"],
        }, {
            "site": "default",
            "stream_id": bs2.state["stream_id"],
        }],
        file_storage_config={
            "directory": backup_target_location,
            "storage_type": "local",
        },
        mysql_client_params=restored_connect_options,
        mysql_config_file_name=mysql_empty.config_name,
        mysql_data_directory=mysql_empty.config_options.datadir,
        mysql_relay_log_index_file=mysql_empty.config_options.relay_log_index_file,
        mysql_relay_log_prefix=mysql_empty.config_options.relay_log_file_prefix,
        pending_binlogs_state_file=state_file_name.replace(".json", "") + ".pending_binlogs",
        restart_mysqld_callback=lambda **kwargs: restart_mysql(mysql_empty, **kwargs),
        rsa_private_key_pem=private_key_pem,
        site="default",
        state_file=state_file_name,
        stats=build_statsd_client(),
        stream_id=bs1.state["stream_id"],
        target_time=pitr_target_time,
        temp_dir=mysql_empty.base_dir,
    )
    # Only allow a few simultaneous binlogs so that we get to test multiple apply rounds even with small data set
    rc.max_binlog_count = 3
    start = time.monotonic()
    rc.start()
    try:
        while True:
            assert time.monotonic() - start < 120
            assert rc.state["phase"] != RestoreCoordinator.Phase.failed
            if rc.state["phase"] == RestoreCoordinator.Phase.completed:
                break

            print("Current restore coordinator phase:", rc.state["phase"])
            time.sleep(1)
    finally:
        rc.stop()

    assert rc.state["restore_errors"] == 0
    assert rc.state["remote_read_errors"] == 0

    with myhoard_util.mysql_cursor(**restored_connect_options) as cursor:
        cursor.execute("SHOW MASTER STATUS")
        final_status = cursor.fetchone()
        print(time.time(), "Restored server's final status", final_status)

        cursor.execute("SELECT COUNT(*) AS count FROM db1.t1")
        expected_row_count = pitr_row_count or data_generator.row_count
        print("Expected row count", expected_row_count)
        actual_row_count = cursor.fetchone()["count"]
        print("Actual row count", actual_row_count)
        for batch_start in range(0, expected_row_count - 1, 200):
            cursor.execute("SELECT id, data FROM db1.t1 WHERE id > %s ORDER BY id ASC LIMIT 200", [batch_start])
            results = cursor.fetchall()
            for i, result in enumerate(results):
                assert result["id"] == batch_start + i + 1
                row_info = data_generator.row_infos[batch_start + i]
                expected_data = row_info[0] * row_info[1]
                assert result["data"] == expected_data

        assert actual_row_count == expected_row_count

        master_status = pitr_master_status or original_master_status
        assert final_status["Executed_Gtid_Set"] == master_status["Executed_Gtid_Set"]
        assert final_status["File"] == "bin.000001"
