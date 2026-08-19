# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from . import build_statsd_client, wait_for_port
from .helpers.version import xtrabackup_version_to_string
from myhoard.basebackup_operation import BasebackupOperation
from myhoard.basebackup_restore_operation import BasebackupRestoreOperation
from packaging.version import Version
from unittest.mock import patch

import myhoard.util as myhoard_util
import os
import pytest
import shutil
import subprocess
import tempfile
import threading
import time

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def _make_restore_op(prepare_progress_callback=None):
    """Minimal BasebackupRestoreOperation for parser unit tests; no subprocess
    is launched."""
    return BasebackupRestoreOperation(
        encryption_algorithm="AES256",
        encryption_key=b"0" * 24,
        free_memory_percentage=80,
        mysql_config_file_name="/dev/null",
        mysql_data_directory="/dev/null",
        stats=build_statsd_client(),
        stream_handler=None,
        target_dir="",
        temp_dir="",
        prepare_progress_callback=prepare_progress_callback,
    )


class TestPrepareOutputParser:
    """The parser only drives progress from the canonical InnoDB scan line:

        InnoDB: Doing recovery: scanned up to log sequence number N

    Other lines mentioning LSNs are intentionally ignored.
    """

    # pylint: disable=protected-access
    def _feed(self, op, lines):
        for line in lines:
            op._process_prepare_output_line(line.encode("utf-8"), "stderr")

    def test_lsn_progress_monotone(self):
        # The first scan line seeds scan_start_lsn (so the window anchors at
        # the first observed LSN, not from_lsn=0); later lines animate across
        # (last_lsn - scan_start).
        captured: list[int | None] = []
        op = _make_restore_op(prepare_progress_callback=lambda **kw: captured.append(kw["pct"]))
        op._prepare_last_lsn = 2000

        self._feed(
            op,
            [
                "InnoDB: Doing recovery: scanned up to log sequence number 1000",
                "InnoDB: Doing recovery: scanned up to log sequence number 1500",
                "InnoDB: Doing recovery: scanned up to log sequence number 1900",
            ],
        )
        assert op.prepare_progress_pct == 90
        assert captured == [0, 50, 90]

    def test_ignores_non_scan_lines(self):
        # Only "Doing recovery: scanned up to log sequence number N" drives progress.
        # Other InnoDB lines that mention LSNs must not advance the bar.
        op = _make_restore_op()
        op._prepare_last_lsn = 1000
        self._feed(
            op,
            [
                "InnoDB: Applying log record at LSN 9999",
                "InnoDB: Starting crash recovery from checkpoint LSN 500",
            ],
        )
        assert op.prepare_progress_pct is None

    def test_pct_never_regresses(self):
        op = _make_restore_op()
        op._prepare_last_lsn = 1000
        self._feed(
            op,
            [
                "InnoDB: Doing recovery: scanned up to log sequence number 500",
                # Out-of-order smaller LSN must not pull the bar back.
                "InnoDB: Doing recovery: scanned up to log sequence number 300",
                "InnoDB: Doing recovery: scanned up to log sequence number 750",
            ],
        )
        assert op.prepare_progress_pct == 50

    def test_pct_pinned_at_100_on_shutdown_completed(self):
        # "Shutdown completed" pins to 100% even when no scan line landed.
        # scan_start gets seeded to prepared_lsn (>= last_lsn typically), so
        # the property's span<=0 branch handles the negative span.
        op = _make_restore_op()
        op._prepare_last_lsn = 1000
        op._process_prepare_output_line(b"Shutdown completed; log sequence number 2000", "stderr")
        assert op.prepared_lsn == 2000
        assert op.prepare_progress_pct == 100

    def test_zero_range_does_not_divide_by_zero(self):
        # One scan line at exactly last_lsn → denominator zero, pin at 100.
        op = _make_restore_op()
        op._prepare_last_lsn = 500
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 500", "stderr")
        assert op.prepare_progress_pct == 100

    def test_no_checkpoints_means_no_pct_and_no_callback(self):
        captured: list[int | None] = []
        op = _make_restore_op(prepare_progress_callback=lambda **kw: captured.append(kw["pct"]))

        self._feed(op, ["InnoDB: Doing recovery: scanned up to log sequence number 100"])
        assert op.prepare_progress_pct is None
        assert not captured

    def test_callback_fires_only_on_pct_change(self):
        # Dedupe at the integer-pct level: scan lines advance the LSN more
        # often than the truncated pct, but the callback must fire once per
        # whole-percent change.
        pcts: list[int | None] = []
        op = _make_restore_op(prepare_progress_callback=lambda **kw: pcts.append(kw["pct"]))
        op._prepare_last_lsn = 1000

        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 500", "stderr")
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 500", "stderr")
        # Different LSN but same truncated pct (0%): must not re-fire.
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 504", "stderr")
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 700", "stderr")
        # 40.6% → truncates to the same 40%: must not re-fire.
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 703", "stderr")
        assert pcts == [0, 40]

    def test_metric_emitted_on_pct_change(self):
        # Each integer pct change emits one gauge sample; the starting marker
        # (lsn=None) bypasses the gauge since None isn't a meaningful value.
        pcts: list[int | None] = []
        op = _make_restore_op(prepare_progress_callback=lambda **kw: pcts.append(kw["pct"]))
        op._prepare_last_lsn = 1000
        with patch.object(op.stats, "gauge_int") as gauge:
            op._set_prepare_current_lsn(None)
            op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 500", "stderr")
            op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 700", "stderr")
            op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 703", "stderr")
        assert gauge.call_args_list == [
            (("myhoard.basebackup_restore.xtrabackup_prepare_progress", 0), {}),
            (("myhoard.basebackup_restore.xtrabackup_prepare_progress", 40), {}),
        ]


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
            progress_pcts: list[int | None] = []
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
                prepare_progress_callback=lambda *, pct: progress_pcts.append(pct),
            )
            restore_op.prepare_backup(checkpoints_file_content=backup_op.checkpoints_file_content)
            restore_op.restore_backup()

        assert restore_op.number_of_files >= backup_op.number_of_files
        assert progress_pcts[0] is None
        assert progress_pcts[-1] == 100
        assert restore_op.prepare_progress_pct == 100

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


@pytest.mark.parametrize("lock_ddl", [myhoard_util.LOCK_DDL_ON, myhoard_util.LOCK_DDL_REDUCED])
def test_backup_and_restore_with_lock_ddl(mysql_master, mysql_empty, lock_ddl: str) -> None:
    """Takes a real backup with the given lock_ddl setting while DDL runs on the server, and restores it.

    DDL during the copy is the only thing `--lock-ddl=REDUCED` changes: ON takes the DDL lock before the
    copy starts and holds it until the copy ends, so DDL simply waits, while REDUCED lets the DDL through
    and defers reconciling the affected tables to `--prepare`. Running the whole backup and restore flow
    for both values is what proves that deferred reconciliation actually produces a restorable server.
    """
    if lock_ddl == myhoard_util.LOCK_DDL_REDUCED:
        xtrabackup_version = myhoard_util.get_xtrabackup_version()
        if xtrabackup_version < (8, 4):
            pytest.skip(f"--lock-ddl=REDUCED needs Percona XtraBackup 8.4+, running {xtrabackup_version}")
        if mysql_master.version < Version("8.4.0"):
            pytest.skip(f"--lock-ddl=REDUCED needs MySQL 8.4+, running {mysql_master.version}")

    baseline_table_count = 5
    baseline_row_count = 15
    ddl_row_count = 10

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("CREATE DATABASE db_test")
        for table_index in range(baseline_table_count):
            cursor.execute(f"CREATE TABLE db_test.baseline{table_index} (id integer primary key)")
            values = ", ".join(f"({value})" for value in range(baseline_row_count))
            cursor.execute(f"INSERT INTO db_test.baseline{table_index} (id) VALUES {values}")
        # Kept separate from the baseline tables so that ALTERing it below doesn't make the assertions on
        # those ambiguous.
        cursor.execute("CREATE TABLE db_test.alter_target (id integer primary key, payload varchar(64))")
        values = ", ".join(f"({value}, 'payload{value}')" for value in range(baseline_row_count))
        cursor.execute(f"INSERT INTO db_test.alter_target (id, payload) VALUES {values}")
        cursor.execute("COMMIT")
        cursor.execute("FLUSH LOGS")

    stop_ddl = threading.Event()
    created_tables: list[str] = []
    ddl_errors: list[Exception] = []

    def run_ddl() -> None:
        # With lock_ddl=ON the very first statement blocks for as long as the copy takes, so this connection
        # needs a read timeout far above the default 4s.
        connect_options = dict(mysql_master.connect_options, timeout=300)
        try:
            with myhoard_util.mysql_cursor(**connect_options) as ddl_cursor:
                # ADD INDEX rewrites an existing data file, which is the case REDUCED has to recopy
                ddl_cursor.execute("ALTER TABLE db_test.alter_target ADD INDEX payload_index (payload)")
                table_index = 0
                while not stop_ddl.is_set():
                    # CREATE TABLE auto-commits and the rows land in a separate transaction, so a
                    # consistent snapshot can hold the table with all of its rows or with none of them
                    table_name = f"ddl{table_index}"
                    ddl_cursor.execute(f"CREATE TABLE db_test.{table_name} (id integer primary key)")
                    values = ", ".join(f"({value})" for value in range(ddl_row_count))
                    ddl_cursor.execute(f"INSERT INTO db_test.{table_name} (id) VALUES {values}")
                    ddl_cursor.execute("COMMIT")
                    created_tables.append(table_name)
                    table_index += 1
                    time.sleep(0.05)
        except Exception as ex:  # pylint: disable=broad-except
            ddl_errors.append(ex)

    encryption_key = os.urandom(24)

    with tempfile.NamedTemporaryFile() as backup_file:

        def output_stream_handler(stream):
            shutil.copyfileobj(stream, backup_file)

        backup_op = BasebackupOperation(
            encryption_algorithm="AES256",
            encryption_key=encryption_key,
            lock_ddl=lock_ddl,
            mysql_client_params=mysql_master.connect_options,
            mysql_config_file_name=mysql_master.config_name,
            mysql_data_directory=mysql_master.config_options.datadir,
            stats=build_statsd_client(),
            stream_handler=output_stream_handler,
            temp_dir=mysql_master.base_dir,
        )

        ddl_thread = threading.Thread(target=run_ddl, name="ddl-during-backup")
        ddl_thread.start()
        try:
            backup_op.create_backup()
        finally:
            stop_ddl.set()
            ddl_thread.join(timeout=300)

        assert not ddl_errors, f"DDL failed while backing up: {ddl_errors}"
        assert not ddl_thread.is_alive()
        if lock_ddl == myhoard_util.LOCK_DDL_REDUCED:
            # The point of REDUCED: DDL is not blocked for the duration of the copy
            assert created_tables, "no DDL got through during the backup, REDUCED did not reduce the lock"

        with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
            cursor.execute(mysql_master.show_binary_logs_status_cmd)
            source_status = cursor.fetchone()
            assert source_status
            source_gtid_executed = source_status["Executed_Gtid_Set"]

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
            restore_op.prepare_backup(checkpoints_file_content=backup_op.checkpoints_file_content)
            restore_op.restore_backup()

        assert restore_op.number_of_files >= backup_op.number_of_files

    mysql_empty.proc = subprocess.Popen(mysql_empty.startup_command)  # pylint: disable=consider-using-with
    wait_for_port(mysql_empty.port)

    with myhoard_util.mysql_cursor(
        password=mysql_master.password,
        port=mysql_empty.port,
        user=mysql_master.user,
    ) as cursor:
        # Everything that was committed before the backup started must be there in full
        for table_index in range(baseline_table_count):
            cursor.execute(f"SELECT id FROM db_test.baseline{table_index}")
            rows = cursor.fetchall()
            assert sorted(row["id"] for row in rows) == list(range(baseline_row_count))

        cursor.execute("SELECT id, payload FROM db_test.alter_target")
        rows = cursor.fetchall()
        assert sorted((row["id"], row["payload"]) for row in rows) == [
            (value, f"payload{value}") for value in range(baseline_row_count)
        ]

        # Tables created while the backup ran may or may not have made the snapshot, but the ones that did
        # must be readable and hold either all of their rows or none, never a partial transaction
        cursor.execute("SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'db_test'")
        restored_ddl_tables = {row["name"] for row in cursor.fetchall() if row["name"].startswith("ddl")}
        assert restored_ddl_tables <= set(created_tables)
        for table_name in sorted(restored_ddl_tables):
            cursor.execute(f"SELECT id FROM db_test.{table_name}")
            rows = cursor.fetchall()
            ids = sorted(row["id"] for row in rows)
            assert ids in ([], list(range(ddl_row_count))), f"{table_name} restored with a partial transaction: {ids}"

        # The restored server must sit at a point that really existed in the source's history
        cursor.execute(mysql_master.show_binary_logs_status_cmd)
        restored_status = cursor.fetchone()
        assert restored_status
        cursor.execute(
            "SELECT GTID_SUBSET(%s, %s) AS is_subset",
            (restored_status["Executed_Gtid_Set"], source_gtid_executed),
        )
        gtid_subset = cursor.fetchone()
        assert gtid_subset
        assert gtid_subset["is_subset"] == 1


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
