# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from . import generate_rsa_key_pair
from datetime import datetime
from hypothesis import given, settings, strategies as st
from time import sleep
from typing import Iterable, List
from unittest.mock import Mock, patch

import collections
import copy
import itertools
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


def _build_gtid_ranges_in_file_order(events: Iterable[myhoard_util.GtidRangeTuple]) -> List[myhoard_util.GtidRangeDict]:
    """The range builder before out of order GNOs were folded. Kept as the reference for in-order input
    and for the set of GTIDs the new builder must cover."""
    ranges: List[myhoard_util.GtidRangeDict] = []
    for timestamp, server_id, server_uuid, gno, _file_position in events:
        if ranges and ranges[-1]["server_uuid"] == server_uuid and ranges[-1]["end"] + 1 == gno:
            ranges[-1]["end"] = gno
            ranges[-1]["end_ts"] = timestamp
        else:
            ranges.append(
                {
                    "end": gno,
                    "end_ts": timestamp,
                    "server_id": server_id,
                    "server_uuid": server_uuid,
                    "start": gno,
                    "start_ts": timestamp,
                }
            )
    return ranges


def _model_gtid_ranges(events: List[myhoard_util.GtidRangeTuple]) -> List[myhoard_util.GtidRangeDict]:
    """What the builder must produce when all disorder fits in the window: for each run of events from
    one server the sorted GNOs collapsed into ranges, timestamps taken from the events at both ends."""
    result: List[myhoard_util.GtidRangeDict] = []
    for server_uuid, run_iter in itertools.groupby(events, key=lambda event: event[2]):
        run = list(run_iter)
        ts_by_gno = {event[3]: event[0] for event in run}
        gnos = sorted(ts_by_gno)
        # Split the sorted GNOs where consecutive numbers stop
        ends = [previous for previous, gno in zip(gnos, gnos[1:]) if gno != previous + 1] + [gnos[-1]]
        starts = [gnos[0]] + [gno for previous, gno in zip(gnos, gnos[1:]) if gno != previous + 1]
        for start, end in zip(starts, ends):
            result.append(
                {
                    "end": end,
                    "end_ts": ts_by_gno[end],
                    "server_id": run[0][1],
                    "server_uuid": server_uuid,
                    "start": start,
                    "start_ts": ts_by_gno[start],
                }
            )
    return result


def _events(gnos: Iterable[int], server_uuid: str = "a", server_id: int = 1) -> List[myhoard_util.GtidRangeTuple]:
    """One event per GNO, in the given order, with timestamps and positions increasing in file order"""
    return [(1000 + index, server_id, server_uuid, gno, 100 * index) for index, gno in enumerate(gnos)]


WINDOW = myhoard_util.GTID_REORDER_WINDOW


@pytest.mark.parametrize(
    "gnos,expected",
    [
        pytest.param([1, 2, 4, 3, 5], [(1, 5, 1000, 1004)], id="adjacent swap"),
        pytest.param([2, 1, 3], [(1, 3, 1001, 1002)], id="swap at file start"),
        pytest.param([1, 2, 3, 5, 6, 7], [(1, 3, 1000, 1002), (5, 7, 1003, 1005)], id="swap across file end"),
        pytest.param([1, 2, 3, 7, 4, 5, 6, 8], [(1, 8, 1000, 1007)], id="transaction written three positions late"),
        pytest.param([1, 2, 500, 501], [(1, 2, 1000, 1001), (500, 501, 1002, 1003)], id="real gap"),
        pytest.param([1, 1 + WINDOW] + list(range(2, 1 + WINDOW)), [(1, 1 + WINDOW, 1000, 1001)], id="jump within window"),
        pytest.param([1, 2 + WINDOW], [(1, 1, 1000, 1000), (2 + WINDOW, 2 + WINDOW, 1001, 1001)], id="jump past window"),
        pytest.param(
            [1, 2 + WINDOW, 2],
            [(1, 1, 1000, 1000), (2, 2, 1002, 1002), (2 + WINDOW, 2 + WINDOW, 1001, 1001)],
            id="late arrival after a jump past the window is not folded",
        ),
        pytest.param([1, 2, 2, 3], [(1, 2, 1000, 1001), (2, 3, 1002, 1003)], id="duplicate GNO"),
    ],
)
def test_build_gtid_ranges_folds_out_of_order_gnos(gnos, expected):
    ranges = list(myhoard_util.build_gtid_ranges(_events(gnos)))
    assert [(rng["start"], rng["end"], rng["start_ts"], rng["end_ts"]) for rng in ranges] == expected
    assert all(rng["server_uuid"] == "a" and rng["server_id"] == 1 for rng in ranges)


def test_build_gtid_ranges_swapped_pairs_produce_one_range():
    gnos = list(range(1, 20001))
    for index in range(500, 20000, 500):
        gnos[index], gnos[index + 1] = gnos[index + 1], gnos[index]
    events = _events(gnos)
    # 39 swapped pairs, each costs three extra ranges when ranges follow file order
    assert len(_build_gtid_ranges_in_file_order(events)) == 118
    assert list(myhoard_util.build_gtid_ranges(events)) == _model_gtid_ranges(events)


def test_build_gtid_ranges_falls_back_to_file_order_when_disorder_exceeds_window():
    # 1..99, then a jump further than the window, then the block that was skipped
    block_moved_past_window = _events(
        list(range(1, 100)) + list(range(100 + WINDOW + 1, 200 + WINDOW + 1)) + list(range(100, 200))
    )
    assert list(myhoard_util.build_gtid_ranges(block_moved_past_window)) == _build_gtid_ranges_in_file_order(
        block_moved_past_window
    )
    too_many_open_ranges = _events(range(1, 2 * (WINDOW + 10), 2))
    assert list(myhoard_util.build_gtid_ranges(too_many_open_ranges)) == _build_gtid_ranges_in_file_order(
        too_many_open_ranges
    )


def test_build_gtid_ranges_starts_new_range_when_server_changes():
    events = _events([1, 2], server_uuid="a") + _events([1, 2], server_uuid="b", server_id=2) + _events([3, 4], "a")
    assert list(myhoard_util.build_gtid_ranges(events)) == _build_gtid_ranges_in_file_order(events)


def test_build_gtid_ranges_many_servers_in_sequence():
    """A long chain of node replacements leaves the transactions of many servers in one file, one run each"""
    in_order: List[myhoard_util.GtidRangeTuple] = []
    with_swaps: List[myhoard_util.GtidRangeTuple] = []
    for server_id in range(1, 41):
        server_uuid = f"server-{server_id}"
        in_order += _events(range(1, 101), server_uuid=server_uuid, server_id=server_id)
        with_swaps += _events([1, 2, 4, 3, 5, 6, 8, 7, 9, 10], server_uuid=server_uuid, server_id=server_id)
    assert list(myhoard_util.build_gtid_ranges(in_order)) == _build_gtid_ranges_in_file_order(in_order)
    ranges = list(myhoard_util.build_gtid_ranges(with_swaps))
    assert len(_build_gtid_ranges_in_file_order(with_swaps)) == 40 * 7
    assert [(rng["server_id"], rng["start"], rng["end"]) for rng in ranges] == [(i, 1, 10) for i in range(1, 41)]


@st.composite
def in_order_events(draw) -> List[myhoard_util.GtidRangeTuple]:
    """Alternating runs of two servers, the GNOs of each server non-decreasing over the whole input, gaps and
    duplicates allowed"""
    events: List[myhoard_util.GtidRangeTuple] = []
    highest = {"a": 0, "b": 0}
    server_uuid = "a"
    for _ in range(draw(st.integers(min_value=1, max_value=4))):
        gnos = sorted(draw(st.lists(st.integers(min_value=0, max_value=200), min_size=1, max_size=200)))
        gnos = [highest[server_uuid] + gno for gno in gnos]
        highest[server_uuid] = gnos[-1]
        events.extend(_events(gnos, server_uuid=server_uuid))
        server_uuid = "b" if server_uuid == "a" else "a"
    return events


@st.composite
def locally_disordered_events(draw) -> List[myhoard_util.GtidRangeTuple]:
    """Consecutive GNOs with up to 10 missing and up to 15 written up to 8 positions late or early, the kind of
    disorder a parallel applier produces. Each GNO moves once, so nothing ends up further than 23 positions
    from where it belongs and everything fits in the window."""
    gnos = list(range(1, draw(st.integers(min_value=12, max_value=300)) + 1))
    for gno in draw(st.lists(st.sampled_from(gnos), max_size=10, unique=True)):
        gnos.remove(gno)
    moved = set()
    for index, distance in draw(
        st.lists(st.tuples(st.integers(min_value=0, max_value=len(gnos) - 1), st.integers(-8, 8)), max_size=15)
    ):
        if gnos[index] in moved:
            continue
        moved.add(gnos[index])
        target = max(0, min(len(gnos) - 1, index + distance))
        gnos.insert(target, gnos.pop(index))
    return _events(gnos)


@st.composite
def arbitrary_events(draw) -> List[myhoard_util.GtidRangeTuple]:
    """Any order of unique GNOs, in runs from two servers"""
    events: List[myhoard_util.GtidRangeTuple] = []
    for server_uuid in draw(st.lists(st.sampled_from(["a", "b"]), min_size=1, max_size=3)):
        gnos = draw(st.permutations(draw(st.lists(st.integers(1, 500), min_size=1, max_size=150, unique=True))))
        events.extend(_events(gnos, server_uuid=server_uuid))
    return events


@given(in_order_events())
@settings(max_examples=300, deadline=None)
def test_build_gtid_ranges_in_order_output_is_unchanged(events):
    assert list(myhoard_util.build_gtid_ranges(events)) == _build_gtid_ranges_in_file_order(events)


@given(locally_disordered_events())
@settings(max_examples=500, deadline=None)
def test_build_gtid_ranges_folds_disorder_that_fits_in_window(events):
    assert list(myhoard_util.build_gtid_ranges(events)) == _model_gtid_ranges(events)


@given(arbitrary_events())
@settings(max_examples=500, deadline=None)
def test_build_gtid_ranges_covers_exactly_the_input_gtids(events):
    ranges = list(myhoard_util.build_gtid_ranges(events))
    reference = _build_gtid_ranges_in_file_order(events)
    assert myhoard_util.partition_sort_and_combine_gtid_ranges(
        ranges
    ) == myhoard_util.partition_sort_and_combine_gtid_ranges(reference)
    # The same GTID can appear in two runs of one server, so a GTID may have more than one timestamp
    timestamps_by_gtid = collections.defaultdict(set)
    for event in events:
        timestamps_by_gtid[(event[2], event[3])].add(event[0])
    for rng in ranges:
        assert rng["start"] <= rng["end"]
        assert rng["start_ts"] in timestamps_by_gtid[(rng["server_uuid"], rng["start"])]
        assert rng["end_ts"] in timestamps_by_gtid[(rng["server_uuid"], rng["end"])]


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
    "with_binlog,with_gtids,expected",
    [
        (
            False,
            False,
            ["--disable-log-bin", "--skip-slave-preserve-commit-order", "--event-scheduler=OFF", "--gtid-mode=OFF"],
        ),
        (False, True, ["--disable-log-bin", "--skip-slave-preserve-commit-order", "--event-scheduler=OFF"]),
        (True, False, ["--gtid-mode=OFF"]),
        # The restart that finalizes a restore. It must produce no options at all so that mysqld goes
        # back to whatever my.cnf says, in particular so the event scheduler is enabled again.
        (True, True, []),
    ],
)
def test_restore_mysqld_options(with_binlog: bool, with_gtids: bool, expected: list[str]) -> None:
    assert myhoard_util.restore_mysqld_options(with_binlog=with_binlog, with_gtids=with_gtids) == expected


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
