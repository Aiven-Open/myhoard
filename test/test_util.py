# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from . import generate_rsa_key_pair
from datetime import datetime
from time import sleep
from typing import List
from unittest.mock import Mock, patch

import copy
import logging
import myhoard.util as myhoard_util
import os
import pymysql
import pytest
import random
import shutil
import subprocess

pytestmark = [pytest.mark.unittest, pytest.mark.all]


@patch("pymysql.connect")
@pytest.mark.parametrize(
    "conn_options",
    [
        {
            "password": "f@keP@ssw0rd",
            "port": 3306,
            "user": "root",
        },
        {
            "password": "f@keP@ssw0rd",
            "port": 3306,
            "require_ssl": True,
            "user": "root",
        },
        {
            "ca_file": "ca-bundle.crt",
            "password": "f@keP@ssw0rd",
            "port": 3306,
            "require_ssl": True,
            "user": "root",
        },
        {
            "ca_file": "ca-bundle.crt",
            "db": "data",
            "host": "localhost",
            "password": "f@keP@ssw0rd",
            "port": 3306,
            "timeout": 10.0,
            "user": "admin",
        },
    ],
)
def test_mysql_cursor(connect_mock, conn_options):
    with myhoard_util.mysql_cursor(**conn_options):
        pass

    timeout = conn_options.get("timeout", 4.0)
    ssl = None
    if conn_options.get("require_ssl"):
        ssl = {"require": True}
    if conn_options.get("ca_file"):
        ssl = {"ca": conn_options["ca_file"]}

    connect_mock.assert_called_once_with(
        charset="utf8mb4",
        connect_timeout=timeout,
        cursorclass=pymysql.cursors.DictCursor,
        db=conn_options.get("db", "mysql"),
        host=conn_options.get("host", "127.0.0.1"),
        password=conn_options["password"],
        read_timeout=timeout,
        port=conn_options["port"],
        ssl=ssl,
        user=conn_options["user"],
        write_timeout=timeout,
    )


def test_rate_tracking_ndigits_calculation():
    window = 10000.0
    while window > 0.0001:
        ndigits = myhoard_util.RateTracker.calculate_default_ndigits(window=window)
        # Generate 1000 timestamps over the range of one window
        timestamps = [i * (window / 1000) for i in range(1000)]
        # Ensure no more than 100 'bins' are generated from these timestamps
        assert len({round(timestamp, ndigits) for timestamp in timestamps}) <= 100
        window /= 3.0


def test_rate_tracker_exception_handling():
    mock_stats = Mock()
    mock_logger = Mock()
    rate_tracker = myhoard_util.RateTracker(log=mock_logger, stats=mock_stats, window=0.5, frequency=0.5, metric_name="foo")
    rate_tracker.start()
    for _ in range(10):
        rate_tracker.increment(50)
        sleep(0.1)

    # this exception is raised in the main thread, so doesn't need special handling
    with pytest.raises(TypeError):
        rate_tracker.increment(0)
        rate_tracker.increment("banana")  # type: ignore
        rate_tracker.increment(0)
        rate_tracker.increment("banana")  # type: ignore

    # ensure no exceptions have been logged yet
    assert len(mock_logger.exception.call_args_list) == 0, mock_logger.exception.call_args_list
    mock_stats.gauge_int = lambda x: None
    sleep(1)
    assert mock_logger.exception.call_args.args == ("Failed to update transfer rate 'foo'",)


def test_rate_tracker():
    mock_stats = Mock()
    rate_tracker = myhoard_util.RateTracker(
        log=logging.getLogger(), stats=mock_stats, window=1, frequency=0.1, metric_name="foo"
    )
    try:
        rate_tracker.start()
        for _ in range(12):
            rate_tracker.increment(50)
            sleep(0.1)
        call_args = mock_stats.gauge_int.call_args.args  # pylint: disable=no-member
        actual_metric, actual_value = call_args
        assert actual_metric == "foo"
        assert 400 < actual_value < 600

        rate_tracker.increment(0)
        sleep(1.1)

        call_args = mock_stats.gauge_int.call_args.args  # pylint: disable=no-member
        actual_metric, actual_value = call_args
        assert actual_metric == "foo"
        assert actual_value < 50
    finally:
        rate_tracker.stop()


def test_read_gtids_from_log():
    fn = os.path.join(os.path.dirname(__file__), "binlog")
    events = [
        (datetime.utcfromtimestamp(event[0]).isoformat(), event[1], event[2], event[3], event[4])
        for event in myhoard_util.read_gtids_from_log(fn)
    ]

    server_uuid = "c1100de1-04f7-11e9-82fd-60f6773756fe"
    server_id = 1
    expected_events = [
        ("2019-01-03T08:31:29", server_id, server_uuid, 1000003, 195),
        ("2019-01-03T08:31:33", server_id, server_uuid, 1000004, 495),
        ("2019-01-03T08:31:37", server_id, server_uuid, 1000005, 795),
        ("2019-01-03T08:31:41", server_id, server_uuid, 1000006, 1095),
        ("2019-01-03T08:31:51", server_id, server_uuid, 1000007, 1395),
        ("2019-01-03T08:32:08", server_id, server_uuid, 1000008, 1695),
        ("2019-01-03T08:32:15", server_id, server_uuid, 1000009, 2027),
        ("2019-01-03T08:32:19", server_id, server_uuid, 1000010, 2327),
    ]
    assert events == expected_events

    ranges = list(myhoard_util.build_gtid_ranges(myhoard_util.read_gtids_from_log(fn)))
    expected_ranges = [
        {
            "end": 1000010,
            "end_ts": 1546504339,
            "server_id": server_id,
            "server_uuid": server_uuid,
            "start": 1000003,
            "start_ts": 1546504289,
        },
    ]
    assert ranges == expected_ranges

    with pytest.raises(ValueError):
        for _ in myhoard_util.read_gtids_from_log(__file__):
            pass

    events = [
        (datetime.utcfromtimestamp(event[0]).isoformat(), event[1], event[2], event[3], event[4])
        for event in myhoard_util.read_gtids_from_log(fn, read_until_time=1546504335)
    ]
    expected_events.pop()
    expected_events.pop()
    assert events == expected_events

    events = [
        (datetime.utcfromtimestamp(event[0]).isoformat(), event[1], event[2], event[3], event[4])
        for event in myhoard_util.read_gtids_from_log(fn, read_until_position=1695)
    ]
    expected_events.pop()
    assert events == expected_events


def test_build_gtid_ranges():
    events = [
        (1000, 1, "a", 1, 100),
        (1001, 1, "a", 2, 200),
        (1002, 1, "a", 4, 300),
        (1003, 2, "b", 5, 400),
        (1004, 2, "b", 6, 500),
    ]
    ranges = list(myhoard_util.build_gtid_ranges(events))
    expected_ranges = [
        {
            "end": 2,
            "end_ts": 1001,
            "server_id": 1,
            "server_uuid": "a",
            "start": 1,
            "start_ts": 1000,
        },
        {
            "end": 4,
            "end_ts": 1002,
            "server_id": 1,
            "server_uuid": "a",
            "start": 4,
            "start_ts": 1002,
        },
        {
            "end": 6,
            "end_ts": 1004,
            "server_id": 2,
            "server_uuid": "b",
            "start": 5,
            "start_ts": 1003,
        },
    ]
    assert ranges == expected_ranges


def build_range_dict(uuid: str, start: int, end: int) -> myhoard_util.GtidRangeDict:
    return {"server_uuid": uuid, "start": start, "end": end, "server_id": 3, "end_ts": 2, "start_ts": 1}


def test_partition_sort_and_combine_gtid_ranges():
    ranges: List[myhoard_util.GtidRangeDict] = [
        build_range_dict(uuid, start, end)
        for (uuid, start, end) in [
            ("uuid1", 1, 3),
            ("uuid1", 6, 7),
            ("uuid1", 8, 8),
            ("uuid2", 10, 12),
            ("uuid2", 4, 9),
            ("uuid1", 2, 2),
            ("uuid1", 2, 4),
        ]
    ]
    result = myhoard_util.partition_sort_and_combine_gtid_ranges(ranges)
    assert result == {"uuid1": [[1, 4], [6, 8]], "uuid2": [[4, 12]]}


def test_first_contains_gtids_not_in_second():
    first: List[myhoard_util.GtidRangeDict] = [
        build_range_dict(uuid, start, end)
        for (uuid, start, end) in [
            ("uuid1", 1, 3),
            ("uuid1", 6, 7),
            ("uuid1", 8, 8),
            ("uuid2", 10, 12),
            ("uuid2", 4, 9),
            ("uuid1", 2, 2),
            ("uuid1", 2, 4),
        ]
    ]
    second = copy.deepcopy(first)
    assert not myhoard_util.first_contains_gtids_not_in_second(first, second)
    second.append(build_range_dict("uuid3", 1, 1))
    assert not myhoard_util.first_contains_gtids_not_in_second(first, second)
    first.append(build_range_dict("uuid4", 1, 1))
    assert myhoard_util.first_contains_gtids_not_in_second(first, second)
    first.pop()
    first[0]["end"] = 11
    assert myhoard_util.first_contains_gtids_not_in_second(first, second)
    second.append(build_range_dict("uuid1", 1, 12))
    assert not myhoard_util.first_contains_gtids_not_in_second(first, second)


def test_parse_gtid_executed_and_truncate_gtid_executed():
    gtid_executed_str = "uuid1:1-6:9:12-20,uuid2:1-30"
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9], [12, 20]], "uuid2": [[1, 30]]}
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:15")
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9], [12, 15]], "uuid2": [[1, 30]]}
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:10")
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9]], "uuid2": [[1, 30]]}
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:9")
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9]], "uuid2": [[1, 30]]}
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:9, uuid2:7")
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9]], "uuid2": [[1, 7]]}
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:8")
    assert gtid_executed == {"uuid1": [[1, 6]], "uuid2": [[1, 30]]}
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid2:1")
    assert gtid_executed == {"uuid1": [[1, 6]], "uuid2": [[1, 1]]}


def test_are_gtids_in_executed_set():
    gtid_executed_str = "uuid1:1-6:9:12-20,uuid2:1-30"
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    assert myhoard_util.are_gtids_in_executed_set(
        gtid_executed,
        [
            {"server_uuid": "uuid1", "start": 18, "end": 18},
            {"server_uuid": "uuid2", "start": 30, "end": 30},
        ],
    )
    assert not myhoard_util.are_gtids_in_executed_set(
        gtid_executed,
        [{"server_uuid": "uuid1", "start": 18, "end": 21}],
    )
    assert myhoard_util.are_gtids_in_executed_set(
        gtid_executed,
        [{"server_uuid": "uuid1", "start": 18, "end": 21}],
        exclude_uuid="uuid1",
    )
    assert not myhoard_util.are_gtids_in_executed_set(
        gtid_executed,
        [{"server_uuid": "uuid3", "start": 1, "end": 1}],
    )


def test_encrypt_decrypt():
    private_key_pem, public_key_pem = generate_rsa_key_pair()
    for size in range(100):
        data = os.urandom(size)
        encrypted = myhoard_util.rsa_encrypt_bytes(public_key_pem, data)
        decrypted = myhoard_util.rsa_decrypt_bytes(private_key_pem, encrypted)
        assert data != encrypted
        assert data == decrypted


class TestDetectRunningProcessId:
    @pytest.fixture
    def cmd_str(self):
        cmd = [
            "/bin/sleep",
            str(random.randint(1000, 5000)),
            str(random.randint(1000, 5000)),
            str(random.randint(1000, 5000)),
        ]
        cmd_str = " ".join(cmd)
        process = subprocess.Popen(cmd)  # pylint: disable=consider-using-with
        yield cmd_str
        process.kill()

    def test_detect_running_process_id(self, cmd_str):
        spawned_id, output_bytes = myhoard_util.detect_running_process_id(cmd_str)
        if spawned_id is None:
            raise AssertionError(f"Could not match command or matched twice:\n{output_bytes.decode('ascii')}")
        no_id, output_bytes = myhoard_util.detect_running_process_id("certainlynosuchprocesscurrentlyrunning")
        assert no_id is None


def test_restart_unexpected_dead_sql_thread() -> None:
    # The thread died
    replica_status = {
        "Replica_SQL_Running": "No",
        "Last_SQL_Error_Timestamp": "2210102 09:42:42",
        "Last_SQL_Errno": "1023",
        "Last_SQL_Error": "Ran out of memory",
    }
    mock_stats = Mock()
    mock_logger = Mock()
    mock_cursor = Mock()
    myhoard_util.restart_unexpected_dead_sql_thread(mock_cursor, replica_status, mock_stats, mock_logger)
    assert mock_stats.increase.call_args.args == ("myhoard.unexpected_sql_thread_starts",)
    assert mock_cursor.execute.call_args.args == ("START REPLICA SQL_THREAD",)

    # It's not running, but MySQL doesn't report a reason it's died
    replica_status = {
        "Replica_SQL_Running": "No",
        "Last_SQL_Error_Timestamp": "2210102 09:42:42",
        "Last_SQL_Errno": "0",
        "Last_SQL_Error": "",
    }
    mock_stats = Mock()
    mock_logger = Mock()
    mock_cursor = Mock()
    myhoard_util.restart_unexpected_dead_sql_thread(mock_cursor, replica_status, mock_stats, mock_logger)
    assert mock_stats.increase.call_args.args == ("myhoard.unexpected_sql_thread_starts",)
    assert mock_cursor.execute.call_args.args == ("START REPLICA SQL_THREAD",)


def test_xtrabackup_version() -> None:
    version = myhoard_util.get_xtrabackup_version()
    assert len(version) >= 3
    assert version < (99, 99, 99), "version is higher than expected"


def test_parse_version() -> None:
    version = myhoard_util.parse_version("8.0.35-3")
    assert version == (8, 0, 35, 3)


def test_parse_xtrabackup_info() -> None:
    raw_xtrabackup_info = """
    name =
    tool_version = 8.0.30-23
    server_version = 8.0.30
    unparsable line
    """
    xtrabackup_info = myhoard_util.parse_xtrabackup_info(raw_xtrabackup_info)
    assert xtrabackup_info == {
        "name": "",
        "tool_version": "8.0.30-23",
        "server_version": "8.0.30",
    }


def test_find_extra_xtrabackup_executables() -> None:
    bin_infos = myhoard_util.find_extra_xtrabackup_executables()
    assert len(bin_infos) == 0
    xtrabackup_path = shutil.which("xtrabackup")
    assert xtrabackup_path is not None
    xtrabackup_dir = os.path.dirname(xtrabackup_path)
    with patch.dict(os.environ, {"PXB_EXTRA_BIN_PATHS": xtrabackup_dir}):
        bin_infos = myhoard_util.find_extra_xtrabackup_executables()
        assert len(bin_infos) == 1
        assert bin_infos[0].path.name == "xtrabackup"
        assert bin_infos[0].version >= (8, 0, 30)


@pytest.mark.parametrize(
    "dow_schedule,result",
    [
        ("abracadabra", ValueError),
        ("", ValueError),
        ("mon,wed", {0, 2}),
        ("sun", {6}),
    ],
)
def test_parse_dow_schedule(dow_schedule: str, result: set[int] | type) -> None:
    if not isinstance(result, set):
        with pytest.raises(result):
            myhoard_util.parse_dow_schedule(dow_schedule)
    else:
        assert myhoard_util.parse_dow_schedule(dow_schedule) == result


@pytest.mark.parametrize(
    "data,path,output",
    [
        (
            {
                "basebackup_info": {
                    "binlog_index": 3894,
                    "encryption_key": "<PLAIN TEXT>",
                }
            },
            ["basebackup_info", "encryption_key"],
            {
                "basebackup_info": {
                    "binlog_index": 3894,
                    "encryption_key": "***MASKED***",
                }
            },
        ),
        (
            {
                "basebackup_info": {
                    "level2": {
                        "binlog_index": 3894,
                        "encryption_key": "<PLAIN TEXT>",
                    }
                }
            },
            ["basebackup_info", "level2", "encryption_key"],
            {
                "basebackup_info": {
                    "level2": {
                        "binlog_index": 3894,
                        "encryption_key": "***MASKED***",
                    }
                }
            },
        ),
        (
            {
                "basebackup_info": {
                    "level2": {
                        "binlog_index": 3894,
                        "encryption_key": "<PLAIN TEXT>",
                    }
                }
            },
            ["basebackup_info", "encryption_key"],
            {
                "basebackup_info": {
                    "level2": {
                        "binlog_index": 3894,
                        "encryption_key": "<PLAIN TEXT>",
                    }
                }
            },
        ),
        (
            {"encryption_key": "<PLAIN TEXT>"},
            ["encryption_key"],
            {"encryption_key": "***MASKED***"},
        ),
    ],
)
def test_mask_fields(data: dict, path: list[str], output: dict) -> None:
    assert myhoard_util.mask_fields(data, path) == output
