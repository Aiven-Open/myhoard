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


def _make_restore_op():
    """Construct a BasebackupRestoreOperation with minimal args for parser unit tests.

    No subprocess is ever launched; we call _process_prepare_output_line directly.
    The parser short-circuits when no callback is attached (so we don't parse lines
    for nothing in production), so we attach a no-op callback by default here.
    Tests that care about the callback reassign it.
    """
    op = BasebackupRestoreOperation(
        encryption_algorithm="AES256",
        encryption_key=b"0" * 24,
        free_memory_percentage=80,
        mysql_config_file_name="/dev/null",
        mysql_data_directory="/dev/null",
        stats=build_statsd_client(),
        stream_handler=None,
        target_dir="",
        temp_dir="",
    )
    op.prepare_progress_callback = lambda **_: None
    return op


class TestPrepareOutputParser:
    """The parser only looks at the canonical InnoDB scan line:

        InnoDB: Doing recovery: scanned up to log sequence number N

    Other lines that happen to contain numbers or mention LSNs are intentionally
    ignored — the scan-line LSN is what tracks recovery progress towards
    last_lsn (the redo stop LSN from the checkpoints file).
    """

    # pylint: disable=protected-access
    def _feed(self, op, lines):
        for line in lines:
            op._process_prepare_output_line(line.encode("utf-8"), "stderr")

    def test_lsn_progress_monotone(self):
        # The first scan line seeds scan_start_lsn, so it always reads as 0% and
        # later lines animate across (last_lsn - scan_start). This models real
        # production: a full backup has from_lsn=0 but we still want the bar to
        # move, so we anchor the window at the first observed scan LSN.
        op = _make_restore_op()
        op.prepare_last_lsn = 2000
        captured: list[int | None] = []
        op.prepare_progress_callback = lambda **kw: captured.append(kw["pct"])

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
        op.prepare_last_lsn = 1000
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
        op.prepare_last_lsn = 1000
        self._feed(
            op,
            [
                "InnoDB: Doing recovery: scanned up to log sequence number 500",
                # Out-of-order smaller LSN must not pull the bar back.
                "InnoDB: Doing recovery: scanned up to log sequence number 300",
                "InnoDB: Doing recovery: scanned up to log sequence number 750",
            ],
        )
        # scan_start=500, last=1000, current=750 → 50%.
        assert op.prepare_progress_pct == 50

    def test_pct_pinned_at_100_on_shutdown_completed(self):
        # The final "Shutdown completed" line pins the bar at 100% even when no
        # intermediate scan line reached last_lsn. scan_start_lsn is still unset
        # here; pinning moves current_lsn to last_lsn, so the property needs to
        # handle the case where scan_start_lsn is None — fall back to "100% if
        # current has reached last_lsn".
        op = _make_restore_op()
        op.prepare_last_lsn = 1000
        op._process_prepare_output_line(  # pylint: disable=protected-access
            b"Shutdown completed; log sequence number 2000", "stderr"
        )
        assert op.prepared_lsn == 2000
        assert op.prepare_progress_pct == 100

    def test_zero_range_does_not_divide_by_zero(self):
        # Only one scan line lands, at exactly last_lsn. scan_start == last_lsn,
        # so the denominator is zero; we pin at 100 instead of dividing.
        op = _make_restore_op()
        op.prepare_last_lsn = 500
        op._process_prepare_output_line(  # pylint: disable=protected-access
            b"InnoDB: Doing recovery: scanned up to log sequence number 500", "stderr"
        )
        assert op.prepare_progress_pct == 100

    def test_no_checkpoints_means_no_pct_and_no_callback(self):
        # Without last_lsn there's no denominator: pct stays None and the callback
        # never fires. Callers treat that as "unknown progress" and can fall back
        # to other signals (e.g. elapsed time).
        op = _make_restore_op()
        captured: list[int | None] = []
        op.prepare_progress_callback = lambda **kw: captured.append(kw["pct"])

        self._feed(op, ["InnoDB: Doing recovery: scanned up to log sequence number 100"])
        assert op.prepare_progress_pct is None
        assert not captured

    def test_callback_fires_only_on_pct_change(self):
        # The callback contract is "once per integer pct change", not "once per
        # scan line". InnoDB emits scan lines much more often than the derived
        # pct advances a whole percent, so we must dedupe at the pct level — not
        # at the LSN level — to keep state-file writes and lock traffic bounded.
        op = _make_restore_op()
        op.prepare_last_lsn = 1000
        pcts: list[int | None] = []
        op.prepare_progress_callback = lambda **kw: pcts.append(kw["pct"])

        # pylint: disable=protected-access
        # Seed scan_start=500 → pct=0.
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 500", "stderr")
        # Same LSN: no advance, no callback.
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 500", "stderr")
        # Different LSN but same truncated pct: (504-500)/(1000-500) = 0%. Must
        # not re-fire despite the LSN having advanced — this is the regression
        # path Copilot flagged in PR #232.
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 504", "stderr")
        # scan_start=500, current=700, last=1000 → 40%; callback fires.
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 700", "stderr")
        # (703-500)/500 = 40.6% → truncates to the same 40%. Must not re-fire.
        op._process_prepare_output_line(b"InnoDB: Doing recovery: scanned up to log sequence number 703", "stderr")
        assert pcts == [0, 40]


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
            progress_pcts: list[int | None] = []
            restore_op.prepare_progress_callback = lambda *, pct: progress_pcts.append(pct)
            restore_op.prepare_backup(checkpoints_file_content=backup_op.checkpoints_file_content)
            restore_op.restore_backup()

        assert restore_op.number_of_files >= backup_op.number_of_files
        # First callback fires with pct=None right before xtrabackup --prepare launches;
        # the final shutdown line pins pct at 100.
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
