# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from . import build_statsd_client, MySQLConfig, restart_mysql
from myhoard.basebackup_operation import (
    BasebackupOperation,
    PAGE_TRACKING_COMPONENT_URN,
    PAGE_TRACKING_MIN_XTRABACKUP_VERSION,
)
from packaging.version import Version
from typing import Any, IO
from unittest import SkipTest
from unittest.mock import mock_open, patch

import myhoard.util as myhoard_util
import os
import pymysql
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
            if mysql_master.version >= Version("8.1.0"):
                cursor.execute(
                    "INSERT INTO mysql.gtid_executed (source_uuid, interval_start, interval_end, gtid_tag) "
                    f"VALUES ('{extra_uuid}', 1, 1, 'myhoard')"
                )
            else:
                cursor.execute(
                    "INSERT INTO mysql.gtid_executed (source_uuid, interval_start, interval_end) "
                    f"VALUES ('{extra_uuid}', 1, 1)"
                )
            cursor.execute("COMMIT")

    if extra_uuid:
        restart_mysql(mysql_master)

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute(mysql_master.show_binary_logs_status_cmd)
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

    assert op.binlog_info
    assert op.binlog_info["gtid"] == master_status["Executed_Gtid_Set"]

    # Taking basebackup might flush binary logs
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute(mysql_master.show_binary_logs_status_cmd)
        master_status = cursor.fetchone()

    assert op.binlog_info
    assert op.binlog_info["file_name"] == master_status["File"]
    assert op.binlog_info["file_position"] == master_status["Position"]

    # Even almost empty backup is at least 1.5 megs due to standard files that are always included
    assert bytes_read[0] > 1.5 * 1024 * 1024

    # Now add incremental backup
    assert op.checkpoints_file_content is not None
    inc_op = BasebackupOperation(
        encryption_algorithm="AES256",
        encryption_key=encryption_key,
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        progress_callback=progress_callback,
        stats=build_statsd_client(),
        stream_handler=stream_handler,
        temp_dir=mysql_master.base_dir,
        incremental_since_checkpoint=op.checkpoints_file_content,
    )
    inc_op.create_backup()
    assert inc_op.checkpoints_file_content is not None

    full_backup_checkpoint = myhoard_util.parse_xtrabackup_info(op.checkpoints_file_content)
    inc_backup_checkpoint = myhoard_util.parse_xtrabackup_info(inc_op.checkpoints_file_content)
    assert full_backup_checkpoint["backup_type"] == "full-backuped"
    assert inc_backup_checkpoint["backup_type"] == "incremental"
    assert full_backup_checkpoint["from_lsn"] == "0"
    assert full_backup_checkpoint["to_lsn"] == inc_backup_checkpoint["from_lsn"]


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
        if Version(version) < Version("8.0.29"):
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


BACKUP_INFO_FILE_TEMPLATE = """
uuid = 4fe5defe-900a-11ed-9921-0261ecef4636
name = blah
tool_name = xtrabackup
tool_command = --defaults-file=/tmp/mysql.conf --backup --compress --no-version-check --stream xbstream --target-dir /tmp/ --extra-lsndir=/lsn/
tool_version = 8.0.30-23.1.aiven
ibbackup_version = 8.0.30-23.1.aiven
server_version = 8.0.30
start_time = 2023-01-09 10:42:26
end_time = 2023-01-09 10:42:28
lock_time = 1
{binlog_line}
innodb_from_lsn = 0
innodb_to_lsn = 31122960
partial = N
incremental = N
format = xbstream
compressed = compressed
encrypted = N
"""


def test_process_binlog_info(mysql_master: MySQLConfig) -> None:
    op = BasebackupOperation(
        encryption_algorithm="AES256",
        encryption_key=os.urandom(24),
        mysql_client_params=mysql_master.connect_options,
        mysql_config_file_name=mysql_master.config_name,
        mysql_data_directory=mysql_master.config_options.datadir,
        optimize_tables_before_backup=True,
        progress_callback=None,
        stats=build_statsd_client(),
        stream_handler=None,
        temp_dir=mysql_master.base_dir,
    )
    # lsn_dir is not specified
    with pytest.raises(AssertionError):
        op._process_xtrabackup_info()  # pylint: disable=protected-access
    assert op.binlog_info is None

    op.lsn_dir = "/tmp/lsn-dir"
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            op._process_xtrabackup_info()  # pylint: disable=protected-access
        assert op.binlog_info is None

    line = "abrakadabra"
    with patch("builtins.open", mock_open(read_data=BACKUP_INFO_FILE_TEMPLATE.format(binlog_line=line))):
        op._process_xtrabackup_info()  # pylint: disable=protected-access
    assert op.binlog_info is None

    line = (
        "binlog_pos = filename 'binlog.000220', position '236', GTID of the last change "
        "'00006582-5ce4-11ea-9748-22f28f5a4c51:1-55419,00ae3f16-75ce-11ea-9b71-7266f0f43b98:1-4104'"
    )
    with patch("builtins.open", mock_open(read_data=BACKUP_INFO_FILE_TEMPLATE.format(binlog_line=line))):
        op._process_xtrabackup_info()  # pylint: disable=protected-access
    assert op.binlog_info == {
        "file_name": "binlog.000220",
        "file_position": 236,
        "gtid": "00006582-5ce4-11ea-9748-22f28f5a4c51:1-55419,00ae3f16-75ce-11ea-9b71-7266f0f43b98:1-4104",
    }

    line = "binlog_pos = filename 'binlog.000221', position '238'"
    with patch("builtins.open", mock_open(read_data=BACKUP_INFO_FILE_TEMPLATE.format(binlog_line=line))):
        op._process_xtrabackup_info()  # pylint: disable=protected-access
    assert op.binlog_info == {
        "file_name": "binlog.000221",
        "file_position": 238,
        "gtid": None,
    }


@pytest.mark.parametrize(
    ("line", "expected_file"),
    [
        # Percona XtraBackup up to 8.4.0-6
        (
            "2026-11-20T10:00:00.1-00:00 3 [Note] [MY-011825] [Xtrabackup] "
            "Compressing, encrypting and streaming ./test0/foo0.ibd",
            "./test0/foo0.ibd",
        ),
        (
            "2026-11-20T10:00:00.1-00:00 0 [Note] [MY-011825] [Xtrabackup] "
            "Compressing, encrypting and streaming mysql/slow_log_215.sdi to <STDOUT>",
            "mysql/slow_log_215.sdi",
        ),
        (
            "2026-11-20T10:00:00.1-00:00 0 [Note] [MY-011825] [Xtrabackup] "
            "Compressing, encrypting and streaming ./bin.000004 to <STDOUT> up to position 197",
            "./bin.000004",
        ),
        # 8.4.0-7 and newer add the file size, and leave out the space before the name of a tablespace file
        (
            "2026-11-20T10:00:00.1-00:00 3 [Note] [MY-011825] [Xtrabackup] space_id: 5, "
            "Compressing, encrypting and streaming./test0/foo0.ibd, size 114688 (112.00 KiB)",
            "./test0/foo0.ibd",
        ),
        (
            "2026-11-20T10:00:00.1-00:00 0 [Note] [MY-011825] [Xtrabackup] "
            "Compressing, encrypting and streaming mysql/slow_log_215.sdi to <STDOUT>, size 12505 (12.21 KiB)",
            "mysql/slow_log_215.sdi",
        ),
        (
            "2026-11-20T10:00:00.1-00:00 0 [Note] [MY-011825] [Xtrabackup] "
            "Compressing, encrypting and streaming ./bin.000004 to <STDOUT> up to position 197, size 197 (197 bytes)",
            "./bin.000004",
        ),
        (
            "2026-11-20T10:00:00.1-00:00 0 [Note] [MY-011825] [Xtrabackup] Compressing, encrypting and streaming <STDOUT>",
            "<STDOUT>",
        ),
    ],
)
def test_process_output_line_new_file(line: str, expected_file: str) -> None:
    op = _make_backup_op()
    assert op._process_output_line_new_file(line)  # pylint: disable=protected-access
    assert op.current_file == expected_file


def _make_backup_op(
    *,
    copy_threads: int = 1,
    estimate_memory: bool = False,
    lock_ddl: str | None = None,
    page_tracking: bool = False,
    throttle: Any = None,
) -> BasebackupOperation:
    # /dev/null is read for the mysql config at construction time; no live server or subprocess is needed.
    op = BasebackupOperation(
        copy_threads=copy_threads,
        encryption_algorithm="AES256",
        encryption_key=b"0" * 24,
        estimate_memory=estimate_memory,
        lock_ddl=lock_ddl,
        mysql_client_params={"host": "127.0.0.1"},
        mysql_config_file_name="/dev/null",
        mysql_data_directory="/dev/null",
        page_tracking=page_tracking,
        stats=build_statsd_client(),
        stream_handler=None,
        temp_dir="/tmp",
        throttle=throttle,
    )
    op.temp_dir = "/tmp/xtrabackup"
    op.lsn_dir = "/tmp/xtrabackupmeta"
    return op


def test_build_backup_command_line_estimate_memory_off_by_default() -> None:
    op = _make_backup_op()
    command_line = op._build_backup_command_line(  # pylint: disable=protected-access
        mysql_config_file_name="/etc/mysql/my.cnf", encryption_key_file_name="/tmp/key.bin"
    )
    assert "--backup" in command_line
    assert "--estimate-memory=ON" not in command_line


def test_build_backup_command_line_estimate_memory_enabled() -> None:
    op = _make_backup_op(estimate_memory=True)
    command_line = op._build_backup_command_line(  # pylint: disable=protected-access
        mysql_config_file_name="/etc/mysql/my.cnf", encryption_key_file_name="/tmp/key.bin"
    )
    assert "--estimate-memory=ON" in command_line


@pytest.mark.parametrize(
    ("lock_ddl", "xtrabackup_version", "expected_option"),
    [
        # not set: the option is left out and xtrabackup's own default applies, so the command line stays
        # what MyHoard has always produced
        (None, (8, 4, 8, 1), None),
        # an explicit ON is passed through so it does not depend on xtrabackup's default
        (myhoard_util.LOCK_DDL_ON, (8, 4, 8, 1), "--lock-ddl=ON"),
        (myhoard_util.LOCK_DDL_ON, (8, 0, 35, 30), "--lock-ddl=ON"),
        (myhoard_util.LOCK_DDL_REDUCED, (8, 4, 8, 1), "--lock-ddl=REDUCED"),
        # the value is not case sensitive
        ("on", (8, 4, 8, 1), "--lock-ddl=ON"),
        ("Reduced", (8, 4, 8, 1), "--lock-ddl=REDUCED"),
        # 8.0 has no REDUCED enum value and refuses to start with it, so it must not be requested
        (myhoard_util.LOCK_DDL_REDUCED, (8, 0, 35, 30), None),
        # a value we don't know must not be passed through to xtrabackup either
        ("BOGUS", (8, 4, 8, 1), None),
    ],
)
def test_build_backup_command_line_lock_ddl(
    lock_ddl: str | None, xtrabackup_version: myhoard_util.BinVersion, expected_option: str | None
) -> None:
    op = _make_backup_op(lock_ddl=lock_ddl)
    with patch("myhoard.basebackup_operation.get_xtrabackup_version", return_value=xtrabackup_version):
        # create_backup resolves it before building the command line
        op.lock_ddl_option = op._resolve_lock_ddl()  # pylint: disable=protected-access
        command_line = op._build_backup_command_line(  # pylint: disable=protected-access
            mysql_config_file_name="/etc/mysql/my.cnf", encryption_key_file_name="/tmp/key.bin"
        )

    lock_ddl_options = [option for option in command_line if option.startswith("--lock-ddl")]
    assert lock_ddl_options == ([expected_option] if expected_option else [])


@pytest.mark.parametrize(
    ("lock_ddl", "xtrabackup_version", "expected_tag"),
    [
        # not set: no --lock-ddl is passed and xtrabackup's default applies, which is ON
        (None, (8, 4, 8, 1), myhoard_util.LOCK_DDL_ON),
        (myhoard_util.LOCK_DDL_ON, (8, 4, 8, 1), myhoard_util.LOCK_DDL_ON),
        (myhoard_util.LOCK_DDL_REDUCED, (8, 4, 8, 1), myhoard_util.LOCK_DDL_REDUCED),
        # REDUCED is not passed on 8.0 so the backup runs with xtrabackup's default, the tag must say so
        (myhoard_util.LOCK_DDL_REDUCED, (8, 0, 35, 30), myhoard_util.LOCK_DDL_ON),
    ],
)
def test_create_backup_tags_metrics_with_lock_ddl(
    tmp_path, lock_ddl: str | None, xtrabackup_version: myhoard_util.BinVersion, expected_tag: str
) -> None:
    op = _make_backup_op(lock_ddl=lock_ddl)
    op.temp_dir_base = str(tmp_path)
    with (
        patch("myhoard.basebackup_operation.get_xtrabackup_version", return_value=xtrabackup_version) as get_version,
        patch("myhoard.basebackup_operation.subprocess.Popen"),
        patch.object(op, "_get_data_directory_size", return_value=(0, 0)),
        patch.object(op, "_process_input_output"),
        patch.object(op.stats, "timing") as timing,
        patch.object(op.stats, "gauge_float") as gauge_float,
    ):
        op.create_backup()

    assert op.lock_ddl_tag == expected_tag
    # the tag and the command line come from the same resolution, done once
    assert get_version.call_count <= 1
    metric, _duration, tags = timing.call_args.args
    assert metric == "myhoard.basebackup.xtrabackup_backup"
    assert tags["lock_ddl"] == expected_tag
    # both the initial and the final progress update, so none goes out untagged
    assert [call.args[:2] for call in gauge_float.call_args_list] == [
        ("myhoard.basebackup.estimated_progress", 0),
        ("myhoard.basebackup.estimated_progress", 100),
    ]
    assert all(call.kwargs["tags"]["lock_ddl"] == expected_tag for call in gauge_float.call_args_list)


def test_create_backup_without_resolved_lock_ddl_has_no_tag(tmp_path) -> None:
    op = _make_backup_op(lock_ddl=myhoard_util.LOCK_DDL_REDUCED)
    op.temp_dir_base = str(tmp_path)
    with (
        patch("myhoard.basebackup_operation.get_xtrabackup_version", side_effect=FileNotFoundError("xtrabackup")),
        patch("myhoard.basebackup_operation.subprocess.Popen") as popen,
        pytest.raises(FileNotFoundError),
    ):
        op.create_backup()

    # xtrabackup never ran, so there is no mode to report
    popen.assert_not_called()
    assert op.lock_ddl_tag is None


@pytest.mark.parametrize(
    ("throttle", "expected_option"),
    [
        # not set: the option is left out, so the command line stays what MyHoard has always produced
        (None, None),
        # 0 is unlimited for xtrabackup as well
        (0, None),
        (1, "--throttle=1"),
        (20, "--throttle=20"),
        # values that are not a non-negative integer must not be passed through to xtrabackup
        (-1, None),
        ("5", None),
        (2.5, None),
        (True, None),
    ],
)
def test_build_backup_command_line_throttle(throttle: Any, expected_option: str | None) -> None:
    op = _make_backup_op(throttle=throttle)
    command_line = op._build_backup_command_line(  # pylint: disable=protected-access
        mysql_config_file_name="/etc/mysql/my.cnf", encryption_key_file_name="/tmp/key.bin"
    )

    throttle_options = [option for option in command_line if option.startswith("--throttle")]
    assert throttle_options == ([expected_option] if expected_option else [])


def test_build_backup_command_line_throttle_with_parallel_copy() -> None:
    # xtrabackup shares one throttle limit between all copy threads, so both options are passed as they are
    op = _make_backup_op(copy_threads=4, throttle=5)
    command_line = op._build_backup_command_line(  # pylint: disable=protected-access
        mysql_config_file_name="/etc/mysql/my.cnf", encryption_key_file_name="/tmp/key.bin"
    )

    assert "--parallel=4" in command_line
    assert "--throttle=5" in command_line


@pytest.mark.parametrize(
    ("page_tracking", "lock_ddl", "xtrabackup_version", "component_installed", "expected"),
    [
        # off by default: the command line stays what MyHoard has always produced
        (False, None, (8, 4, 0, 7), True, False),
        (True, None, (8, 4, 0, 7), True, True),
        (True, myhoard_util.LOCK_DDL_ON, (8, 4, 0, 8), True, True),
        # xtrabackup refuses --page-tracking together with --lock-ddl=REDUCED
        (True, myhoard_util.LOCK_DDL_REDUCED, (8, 4, 0, 7), True, False),
        # before 8.4.0-7 page tracking also read unchanged page ranges
        (True, None, (8, 4, 0, 6), True, False),
        (True, None, (8, 0, 35, 36), True, False),
        # the server component is missing and could not be installed
        (True, None, (8, 4, 0, 7), False, False),
    ],
)
def test_build_backup_command_line_page_tracking(
    page_tracking: bool,
    lock_ddl: str | None,
    xtrabackup_version: myhoard_util.BinVersion,
    component_installed: bool,
    expected: bool,
) -> None:
    op = _make_backup_op(lock_ddl=lock_ddl, page_tracking=page_tracking)
    with (
        patch("myhoard.basebackup_operation.get_xtrabackup_version", return_value=xtrabackup_version),
        patch.object(op, "_ensure_page_tracking_component", return_value=component_installed),
    ):
        # create_backup resolves both before building the command line
        op.lock_ddl_option = op._resolve_lock_ddl()  # pylint: disable=protected-access
        op.page_tracking_enabled = op._resolve_page_tracking()  # pylint: disable=protected-access
        command_line = op._build_backup_command_line(  # pylint: disable=protected-access
            mysql_config_file_name="/etc/mysql/my.cnf", encryption_key_file_name="/tmp/key.bin"
        )

    assert op.page_tracking_enabled is expected
    assert ("--page-tracking" in command_line) is expected


def test_page_tracking_off_does_not_touch_the_server() -> None:
    op = _make_backup_op()
    with (
        patch("myhoard.basebackup_operation.get_xtrabackup_version") as get_version,
        patch("myhoard.basebackup_operation.mysql_cursor") as mysql_cursor,
    ):
        assert op._resolve_page_tracking() is False  # pylint: disable=protected-access

    get_version.assert_not_called()
    mysql_cursor.assert_not_called()


@pytest.mark.parametrize(
    ("installed_count", "install_error", "expected_install", "expected"),
    [
        (1, None, False, True),
        (0, None, True, True),
        # for example a server build that does not ship the component
        (0, pymysql.err.OperationalError(1126, "Can't open shared library"), True, False),
    ],
)
def test_ensure_page_tracking_component(
    installed_count: int, install_error: Exception | None, expected_install: bool, expected: bool
) -> None:
    install_statement = "INSTALL COMPONENT 'file://component_mysqlbackup'"

    def execute(statement: str, _args: Any = None) -> None:
        if statement == install_statement and install_error:
            raise install_error

    op = _make_backup_op(page_tracking=True)
    with patch("myhoard.basebackup_operation.mysql_cursor") as mysql_cursor:
        cursor = mysql_cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = execute
        cursor.fetchone.return_value = {"count": installed_count}
        assert op._ensure_page_tracking_component() is expected  # pylint: disable=protected-access

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert (install_statement in statements) is expected_install


def test_ensure_page_tracking_component_server_unreachable() -> None:
    op = _make_backup_op(page_tracking=True)
    error = pymysql.err.OperationalError(2003, "Can't connect to MySQL server")
    with patch("myhoard.basebackup_operation.mysql_cursor", side_effect=error):
        assert op._ensure_page_tracking_component() is False  # pylint: disable=protected-access


def test_backup_with_page_tracking(mysql_master: MySQLConfig, caplog: pytest.LogCaptureFixture) -> None:
    if myhoard_util.get_xtrabackup_version() < PAGE_TRACKING_MIN_XTRABACKUP_VERSION:
        raise SkipTest("xtrabackup is too old for page tracking")

    def component_count() -> int:
        with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM mysql.component WHERE component_urn = %s", [PAGE_TRACKING_COMPONENT_URN]
            )
            row = cursor.fetchone()
            assert row
            return row["count"]

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("CREATE DATABASE tracked")
        cursor.execute("CREATE TABLE tracked.t (id INTEGER PRIMARY KEY, value VARCHAR(100))")
        cursor.execute("INSERT INTO tracked.t (id, value) VALUES (1, 'before the full backup')")
        cursor.execute("COMMIT")
    assert component_count() == 0

    def stream_handler(stream: IO[bytes]) -> None:
        while stream.read(10 * 1024):
            pass

    def make_op(incremental_since_checkpoint: str | None = None) -> BasebackupOperation:
        return BasebackupOperation(
            encryption_algorithm="AES256",
            encryption_key=os.urandom(24),
            mysql_client_params=mysql_master.connect_options,
            mysql_config_file_name=mysql_master.config_name,
            mysql_data_directory=mysql_master.config_options.datadir,
            page_tracking=True,
            stats=build_statsd_client(),
            stream_handler=stream_handler,
            temp_dir=mysql_master.base_dir,
            incremental_since_checkpoint=incremental_since_checkpoint,
        )

    op = make_op()
    op.create_backup()
    assert op.page_tracking_enabled

    assert component_count() == 1
    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("INSERT INTO tracked.t (id, value) VALUES (2, 'after the full backup')")
        cursor.execute("COMMIT")

    assert op.checkpoints_file_content is not None
    caplog.clear()
    with caplog.at_level("INFO", logger="BasebackupOperation"):
        inc_op = make_op(incremental_since_checkpoint=op.checkpoints_file_content)
        inc_op.create_backup()
    assert inc_op.page_tracking_enabled
    assert inc_op.checkpoints_file_content is not None
    assert myhoard_util.parse_xtrabackup_info(inc_op.checkpoints_file_content)["backup_type"] == "incremental"
    # xtrabackup says so when it uses the page list, and it says something else when it falls back to the full scan
    assert "Using pagetracking feature for incremental backup" in caplog.text
