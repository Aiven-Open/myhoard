# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from . import build_statsd_client, DataGenerator, generate_rsa_key_pair, restart_mysql, while_asserts
from myhoard.backup_stream import BackupStream
from myhoard.binlog_scanner import BinlogScanner
from myhoard.restore_coordinator import RestoreCoordinator
from myhoard.state_manager import StateManager
from rohmu.object_storage.local import LocalTransfer
from typing import Any, cast, Generic, List, Mapping, TypeVar
from unittest.mock import Mock, patch

import myhoard.util as myhoard_util
import os
import pytest
import time

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def test_restore_coordinator(session_tmpdir, mysql_master, mysql_empty):
    _restore_coordinator_sequence(
        session_tmpdir, mysql_master, mysql_empty, pitr=False, rebuild_tables=False, fail_and_resume=False
    )


def test_restore_coordinator_pitr(session_tmpdir, mysql_master, mysql_empty):
    _restore_coordinator_sequence(
        session_tmpdir, mysql_master, mysql_empty, pitr=True, rebuild_tables=False, fail_and_resume=False
    )


def test_restore_coordinator_rebuild_tables(session_tmpdir, mysql_master, mysql_empty):
    _restore_coordinator_sequence(
        session_tmpdir, mysql_master, mysql_empty, pitr=False, rebuild_tables=True, fail_and_resume=False
    )


def test_restore_coordinator_resume_rebuild_tables(session_tmpdir, mysql_master, mysql_empty):
    _restore_coordinator_sequence(
        session_tmpdir, mysql_master, mysql_empty, pitr=False, rebuild_tables=True, fail_and_resume=True
    )


def _restore_coordinator_sequence(
    session_tmpdir, mysql_master, mysql_empty, *, pitr: bool, rebuild_tables: bool, fail_and_resume: bool
):
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("CREATE DATABASE db1")
        cursor.execute("USE db1")
        cursor.execute("CREATE TABLE t0 (id INTEGER PRIMARY KEY, data TEXT)")
        cursor.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, data TEXT)")
        cursor.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, data TEXT)")
        cursor.execute("SET SESSION sql_require_primary_key = false")
        cursor.execute("SET SESSION sql_mode = ''")
        cursor.execute("CREATE TABLE bad0 (id INTEGER, data TEXT)")
        cursor.execute("CREATE TABLE bad1 (id INTEGER, data DATETIME default '0000-00-00 00:00:00')")
        # Tables with compact rows and a row length longer than 8126 bytes raise an error unless strict mode is disabled
        cursor.execute("SET SESSION innodb_strict_mode = false")
        columns = ", ".join([f"d{i} CHAR(255)" for i in range(40)])
        cursor.execute(f"CREATE TABLE bad2 ({columns}) ROW_FORMAT=COMPACT")
        cursor.execute("COMMIT")

    private_key_pem, public_key_pem = generate_rsa_key_pair()
    backup_target_location = session_tmpdir().strpath
    state_file_name1 = os.path.join(session_tmpdir().strpath, "backup_stream1.json")
    state_file_name2 = os.path.join(session_tmpdir().strpath, "backup_stream2.json")
    remote_binlogs_state_file_name1 = os.path.join(session_tmpdir().strpath, "backup_stream1.remote_binlogs")
    remote_binlogs_state_file_name2 = os.path.join(session_tmpdir().strpath, "backup_stream2.remote_binlogs")
    bs1 = BackupStream(
        backup_reason=BackupStream.BackupReason.requested,
        file_storage_setup_fn=lambda: LocalTransfer(backup_target_location),
        mode=BackupStream.Mode.active,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        normalized_backup_time="2019-02-25T08:20",
        rsa_public_key_pem=public_key_pem,
        remote_binlogs_state_file=remote_binlogs_state_file_name1,
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
        remote_binlogs_state_file=remote_binlogs_state_file_name2,
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
        "initial rows:",
        data_generator.row_count,
        "estimated bytes:",
        data_generator.estimated_bytes,
        "binlog count:",
        len(scanner.binlogs),
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
        "rows when basebackup 1 finished:",
        data_generator.row_count,
        "estimated bytes:",
        data_generator.estimated_bytes,
        "binlog count:",
        len(scanner.binlogs),
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
        "rows when basebackup 2 finished:",
        data_generator.row_count,
        "estimated bytes:",
        data_generator.estimated_bytes,
        "binlog count:",
        len(scanner.binlogs),
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
        "rows when marking complete:",
        data_generator.row_count,
        "estimated bytes:",
        data_generator.estimated_bytes,
        "binlog count:",
        len(scanner.binlogs),
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
        "final rows:",
        data_generator.row_count,
        "estimated bytes:",
        data_generator.estimated_bytes,
        "binlog count:",
        len(scanner.binlogs),
    )  # yapf: disable

    start = time.monotonic()
    while bs2.state["pending_binlogs"]:
        assert time.monotonic() - start < 10
        time.sleep(0.1)

    bs2.stop()

    assert bs2.state["active_details"]["phase"] == BackupStream.ActivePhase.binlog

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("SHOW MASTER STATUS")
        original_master_status = cast(dict, cursor.fetchone())
        print("Original master status", original_master_status)
        cursor.execute("SELECT COUNT(*) AS count FROM db1.t1")
        original_count = cast(dict, cursor.fetchone())["count"]
        print("Number of rows in original master", original_count)

    restored_connect_options = {
        "host": "127.0.0.1",
        "password": mysql_master.password,
        "port": mysql_empty.port,
        "require_ssl": mysql_master.connect_options["require_ssl"],
        "user": mysql_master.user,
    }
    state_file_name = os.path.join(session_tmpdir().strpath, "restore_coordinator.json")
    # Restore basebackup from backup stream 1 but binlogs from both streams. First stream doesn't
    # contain latest state but applying binlogs from second stream should get it fully up-to-date
    rc = RestoreCoordinator(
        binlog_streams=[
            {"site": "default", "stream_id": bs1.state["stream_id"]},
            {"site": "default", "stream_id": bs2.state["stream_id"]},
        ],
        download_workers_count=2,
        file_storage_config={
            "directory": backup_target_location,
            "storage_type": "local",
        },
        free_memory_percentage=80,
        mysql_client_params=restored_connect_options,
        mysql_config_file_name=mysql_empty.config_name,
        mysql_data_directory=mysql_empty.config_options.datadir,
        mysql_relay_log_index_file=mysql_empty.config_options.relay_log_index_file,
        mysql_relay_log_prefix=mysql_empty.config_options.relay_log_file_prefix,
        pending_binlogs_state_file=state_file_name.replace(".json", "") + ".pending_binlogs",
        rebuild_tables=rebuild_tables,
        restart_mysqld_callback=lambda **kwargs: restart_mysql(mysql_empty, **kwargs),
        rsa_private_key_pem=private_key_pem,
        site="default",
        state_file=state_file_name,
        state_manager_class=FailingStateManager if fail_and_resume else StateManager,
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

    if fail_and_resume:
        assert isinstance(rc.state_manager, FailingStateManager)
        assert rc.state["restore_errors"] == 1
        # FailingStateManager is configured to fail once on t1, so it will retry t1.
        # If resume is correctly implemented, it won't retry t0
        assert rc.state_manager.update_log.count({"last_rebuilt_table": "`db1`.`t0`"}) == 1
        assert rc.state_manager.update_log.count({"last_rebuilt_table": "`db1`.`t1`"}) == 2
        assert rc.state_manager.update_log.count({"last_rebuilt_table": "`db1`.`t2`"}) == 1
        assert rc.state_manager.update_log.count({"last_rebuilt_table": "`db1`.`bad0`"}) == 1
        assert rc.state_manager.update_log.count({"last_rebuilt_table": "`db1`.`bad1`"}) == 1
    else:
        assert rc.state["restore_errors"] == 0
    assert rc.state["remote_read_errors"] == 0

    with myhoard_util.mysql_cursor(**restored_connect_options) as cursor:
        cursor.execute("SHOW MASTER STATUS")
        final_status = cast(dict, cursor.fetchone())
        print(time.time(), "Restored server's final status", final_status)

        cursor.execute("SELECT COUNT(*) AS count FROM db1.t1")
        expected_row_count = pitr_row_count or data_generator.row_count
        print("Expected row count", expected_row_count)
        actual_row_count = cast(dict, cursor.fetchone())["count"]
        print("Actual row count", actual_row_count)
        for batch_start in range(0, expected_row_count - 1, 200):
            cursor.execute("SELECT id, data FROM db1.t1 WHERE id > %s ORDER BY id ASC LIMIT 200", [batch_start])
            results = cursor.fetchall()
            for i, result in enumerate(results):
                assert result["id"] == batch_start + i + 1, "Id doesn't match"
                row_info = data_generator.row_infos[batch_start + i]
                expected_data = row_info[0] * row_info[1]
                assert result["data"] == expected_data, "expected_data doesn't match"

        assert actual_row_count == expected_row_count, "row count doesn't match"

        master_status = pitr_master_status or original_master_status
        assert (
            final_status["Executed_Gtid_Set"] == master_status["Executed_Gtid_Set"]
        ), "final status executed_gtid_set differs from master status"
        assert final_status["File"] == "bin.000001", "final status file differes from expected"


@pytest.mark.parametrize(
    "running_state",
    ["Slave has read all relay log; waiting for more updates", "Replica has read all relay log; waiting for more updates"],
)
def test_empty_last_relay(running_state, session_tmpdir, mysql_master, mysql_empty):
    restored_connect_options = {
        "host": "127.0.0.1",
        "password": mysql_master.password,
        "port": mysql_empty.port,
        "require_ssl": mysql_master.connect_options["require_ssl"],
        "user": mysql_master.user,
    }
    state_file_name = os.path.join(session_tmpdir().strpath, "restore_coordinator.json")
    private_key_pem, _ = generate_rsa_key_pair()

    slave_status_response = {"Relay_Log_File": "relay.000001", "Slave_SQL_Running_State": running_state}

    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = slave_status_response

    with patch.object(RestoreCoordinator, "_mysql_cursor") as mock_mysql_cursor:
        mock_mysql_cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.foo.return_value.bar.return_value.something.return_value = "bar"
        mock_cursor.foo.return_value = "bar"
        # We do not connect to the db in this call we just need a RestoreCoordinator object
        rc = RestoreCoordinator(
            binlog_streams=[],
            download_workers_count=2,
            file_storage_config={},
            free_memory_percentage=80,
            mysql_client_params=restored_connect_options,
            mysql_config_file_name=mysql_empty.config_name,
            mysql_data_directory=mysql_empty.config_options.datadir,
            mysql_relay_log_index_file=mysql_empty.config_options.relay_log_index_file,
            mysql_relay_log_prefix=mysql_empty.config_options.relay_log_file_prefix,
            pending_binlogs_state_file=state_file_name.replace(".json", "") + ".pending_binlogs",
            rebuild_tables=False,
            restart_mysqld_callback=lambda **kwargs: restart_mysql(mysql_empty, **kwargs),
            rsa_private_key_pem=private_key_pem,
            site="default",
            state_file=state_file_name,
            stats=build_statsd_client(),
            stream_id="PLACEHOLDER",
            target_time=None,
            temp_dir=mysql_empty.base_dir,
        )

        rc.state["current_relay_log_target"] = 2

        apply_finished, current_index = rc._check_sql_slave_status()  # pylint: disable=protected-access

    assert apply_finished
    assert current_index == 2


class InjectedError(Exception):
    pass


T = TypeVar("T", bound=Mapping[str, Any])


class FailingStateManager(Generic[T], StateManager[T]):
    def __init__(self, *, allow_unknown_keys: bool = False, lock=None, state: T, state_file) -> None:
        super().__init__(allow_unknown_keys=allow_unknown_keys, lock=lock, state=state, state_file=state_file)
        self.update_log: List[Mapping[str, Any]] = []
        self.has_failed = False

    def update_state(self, **kwargs) -> None:
        self.update_log.append(kwargs)
        super().update_state(**kwargs)
        # We're not testing "what if the StateManager fails", but using it as an injection
        # mechanism to stop at interesting points, that's why we raise after writing the state.
        if not self.has_failed and kwargs.get("last_rebuilt_table") == "`db1`.`t1`":
            self.has_failed = True
            raise InjectedError()
