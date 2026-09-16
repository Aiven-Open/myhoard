# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
"""What the gtid_ranges of a binlog with out of order GNOs must mean to the code that reads them.

The parallel applier of a replica can write a transaction into the replica's own binlog after
transactions with higher GNOs. These tests pin the contract between build_gtid_ranges and its
consumers in the restore coordinator, the point in time recovery skip check and the apply completion
check, using the cases raised in the review of https://github.com/Aiven-Open/myhoard/pull/252, and
the edge cases around them: the ends of a file, a swap straddling two files, timestamps that follow
the source's clock, several servers in one file and the old restore state format.

The module is self contained so that the same file runs unchanged against master."""
from . import build_statsd_client
from contextlib import contextmanager, ExitStack
from hypothesis import given, settings, strategies as st
from myhoard.restore_coordinator import PendingBinlogInfo, RestoreCoordinator
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from unittest.mock import Mock, patch

import itertools
import myhoard.restore_coordinator
import myhoard.util as myhoard_util
import os
import pytest
import struct
import uuid

pytestmark = [pytest.mark.unittest, pytest.mark.all]

SERVER_UUID = "0f6b3a2e-0000-4000-8000-000000000001"
OTHER_UUID = "0f6b3a2e-0000-4000-8000-000000000002"
SERVER_ID = 1
MAX_GNO = 2**63 - 1


def _events(
    gnos: Iterable[int], server_uuid: str = SERVER_UUID, server_id: int = SERVER_ID
) -> List[myhoard_util.GtidRangeTuple]:
    """One event per GNO in the given file order, with timestamps 1000, 1001, ... in file order"""
    return [(1000 + index, server_id, server_uuid, gno, 100 * index) for index, gno in enumerate(gnos)]


def _timed_events(timestamps_and_gnos: Iterable[Tuple[int, int]]) -> List[myhoard_util.GtidRangeTuple]:
    """One event per (timestamp, GNO) pair in the given file order. Timestamps need not grow along the file:
    a replica stamps the events it writes with the clock of the source, so a swapped pair can go backwards."""
    return [(ts, SERVER_ID, SERVER_UUID, gno, 100 * index) for index, (ts, gno) in enumerate(timestamps_and_gnos)]


def _ranges(events_or_gnos) -> List[myhoard_util.GtidRangeDict]:
    events = events_or_gnos if events_or_gnos and isinstance(events_or_gnos[0], tuple) else _events(events_or_gnos)
    return list(myhoard_util.build_gtid_ranges(events))


def _bounds(ranges: Iterable[myhoard_util.GtidRangeDict]) -> List[Tuple[int, int]]:
    return [(rng["start"], rng["end"]) for rng in ranges]


def _bounds_and_ts(ranges: Iterable[myhoard_util.GtidRangeDict]) -> List[Tuple[int, int, int, int]]:
    return [(rng["start"], rng["end"], rng["start_ts"], rng["end_ts"]) for rng in ranges]


def _sorted_gno_bounds(gnos: Iterable[int]) -> List[Tuple[int, int]]:
    """The GNOs sorted and collapsed into ranges, the most compact representation of one server's run"""
    bounds: List[Tuple[int, int]] = []
    for gno in sorted(set(gnos)):
        if bounds and bounds[-1][1] + 1 == gno:
            bounds[-1] = (bounds[-1][0], gno)
        else:
            bounds.append((gno, gno))
    return bounds


def _model(events: List[myhoard_util.GtidRangeTuple]) -> List[myhoard_util.GtidRangeDict]:
    """The expected output for runs without repeated GNOs: per run of one server the sorted GNOs collapsed
    into ranges, start_ts and end_ts the earliest and latest timestamp of the events of the range"""
    result: List[myhoard_util.GtidRangeDict] = []
    for server_uuid, run_iter in itertools.groupby(events, key=lambda event: event[2]):
        run = list(run_iter)
        ts_by_gno = {event[3]: event[0] for event in run}
        for start, end in _sorted_gno_bounds(ts_by_gno):
            timestamps = [ts_by_gno[gno] for gno in range(start, end + 1)]
            result.append(
                {
                    "end": end,
                    "end_ts": max(timestamps),
                    "server_id": run[0][1],
                    "server_uuid": server_uuid,
                    "start": start,
                    "start_ts": min(timestamps),
                }
            )
    return result


def _gtids_of_events(events: Iterable[myhoard_util.GtidRangeTuple]) -> Set[Tuple[str, int]]:
    return {(event[2], event[3]) for event in events}


def _gtids_of_ranges(ranges: Iterable[myhoard_util.GtidRangeDict]) -> Set[Tuple[str, int]]:
    return {(rng["server_uuid"], gno) for rng in ranges for gno in range(rng["start"], rng["end"] + 1)}


def _assert_timestamps_envelope_the_events(
    events: List[myhoard_util.GtidRangeTuple], ranges: List[myhoard_util.GtidRangeDict]
) -> None:
    for rng in ranges:
        inside = [
            timestamp
            for timestamp, _, server_uuid, gno, _ in events
            if server_uuid == rng["server_uuid"] and rng["start"] <= gno <= rng["end"]
        ]
        assert inside, rng
        assert rng["start_ts"] <= min(inside), (rng, inside)
        assert rng["end_ts"] >= max(inside), (rng, inside)


# Review comment 1 on util.py: start_ts and end_ts are no longer an envelope over the range


@pytest.mark.parametrize(
    "gnos",
    [
        pytest.param([2, 1, 3], id="lowest GNO written second, its timestamp is not the earliest"),
        pytest.param([1, 2, 4, 5, 3], id="GNO written late, its timestamp is not the latest"),
    ],
)
def test_range_timestamps_envelope_every_event_folded_into_the_range(gnos):
    """The restore coordinator skips a binlog when the start_ts of its first range is at or after the
    target time and marks the target time reached when the end_ts is, so [start_ts, end_ts] must
    cover the commit time of every transaction in the range"""
    events = _events(gnos)
    _assert_timestamps_envelope_the_events(events, list(myhoard_util.build_gtid_ranges(events)))


@given(st.permutations(range(1, 13)))
@settings(max_examples=300, deadline=None)
def test_range_timestamps_envelope_every_event_in_any_order(gnos):
    events = _events(gnos)
    _assert_timestamps_envelope_the_events(events, list(myhoard_util.build_gtid_ranges(events)))


# Review comment 3 on util.py: the cap on open ranges and the distance check fragment and reorder the output


@pytest.mark.parametrize(
    "gnos,open_range_cap",
    [
        pytest.param([1, 3, 5, 7, 4], 3, id="closing ranges at the cap must not split or reorder them"),
        pytest.param([1, 2, 1030, 3], None, id="a jump past the window must not keep a late GNO from its neighbours"),
    ],
)
def test_ranges_are_as_compact_as_the_sorted_gnos_and_in_gno_order(gnos, open_range_cap):
    """build_gtid_ranges promises ranges in GNO order and folds GNOs written out of order into one range.
    The output must be the sorted GNOs of the run collapsed into ranges, no matter how the builder bounds
    its memory. The cap is lowered here so that a five event input hits it."""
    if open_range_cap is None:
        ranges = _ranges(gnos)
    else:
        with patch.object(myhoard_util, "GTID_REORDER_WINDOW", open_range_cap, create=True):
            ranges = _ranges(gnos)
    assert _bounds(ranges) == _sorted_gno_bounds(gnos)


# Edge cases of the builder


@pytest.mark.parametrize(
    "events,expected",
    [
        pytest.param([], [], id="empty file"),
        pytest.param(_events([7]), [(7, 7, 1000, 1000)], id="single transaction"),
        pytest.param(_events([1, 3, 2]), [(1, 3, 1000, 1002)], id="swap at file end"),
        pytest.param(_events([2, 1]), [(1, 2, 1000, 1001)], id="GNO 1 written second"),
        pytest.param(_events([MAX_GNO - 1, MAX_GNO]), [(MAX_GNO - 1, MAX_GNO, 1000, 1001)], id="largest GNO"),
        pytest.param(_events([MAX_GNO, MAX_GNO - 1]), [(MAX_GNO - 1, MAX_GNO, 1000, 1001)], id="largest GNO swapped"),
        pytest.param(
            _timed_events([(1001, 2), (1000, 1)]),
            [(1, 2, 1000, 1001)],
            id="source clock: the late transaction carries the earlier timestamp",
        ),
        pytest.param(
            _timed_events([(1000, 1), (1002, 3), (1001, 2), (1003, 4)]),
            [(1, 4, 1000, 1003)],
            id="source clock: timestamps go backwards in the middle of the file",
        ),
        pytest.param(
            _timed_events([(1000, 1), (1000, 3), (1000, 2)]),
            [(1, 3, 1000, 1000)],
            id="whole swap inside one second",
        ),
    ],
)
def test_builder_edge_cases(events, expected):
    assert _bounds_and_ts(myhoard_util.build_gtid_ranges(events)) == expected


def test_folding_stays_within_a_run_of_one_server():
    """A late GNO that arrives after another server's transactions starts its own range. The run boundary
    is what keeps in-order output identical to the builder before folding."""
    events = _events([1, 3], SERVER_UUID) + _events([1], OTHER_UUID, 2) + _events([2], SERVER_UUID)
    ranges = list(myhoard_util.build_gtid_ranges(events))
    assert [(rng["server_uuid"], rng["start"], rng["end"]) for rng in ranges] == [
        (SERVER_UUID, 1, 1),
        (SERVER_UUID, 3, 3),
        (OTHER_UUID, 1, 1),
        (SERVER_UUID, 2, 2),
    ]
    assert myhoard_util.partition_sort_and_combine_gtid_ranges(ranges) == {SERVER_UUID: [[1, 3]], OTHER_UUID: [[1, 1]]}


def test_swap_across_a_file_boundary_folds_per_file_and_combines_across_files():
    """GNO 5 is the last transaction of the first file and GNO 4 the first of the second. Each file gets one
    extra range, the set helpers combine them, and the upload dedup still sees the second file as new."""
    first_file = _ranges([1, 2, 3, 5])
    second_file = _ranges([4, 6, 7])
    assert _bounds(first_file) == [(1, 3), (5, 5)]
    assert _bounds(second_file) == [(4, 4), (6, 7)]
    assert myhoard_util.add_gtid_ranges_to_executed_set({}, first_file, second_file) == {SERVER_UUID: [[1, 7]]}
    assert myhoard_util.first_contains_gtids_not_in_second(second_file, first_file)
    # A file that holds only the late GNO adds nothing when the remote already covers a higher GNO
    assert not myhoard_util.first_contains_gtids_not_in_second(_ranges([4]), first_file)
    assert myhoard_util.are_gtids_in_executed_set({SERVER_UUID: [[1, 7]]}, second_file)


def test_repeated_gno_inside_a_pending_range_still_covers_exactly_the_input():
    """A GNO repeated inside a pending range is not detected and yields overlapping ranges. The set of GTIDs
    is still exact and the set helpers combine the overlap, as they did for the builder before folding."""
    events = _events([1, 5, 6, 7, 6, 2])
    ranges = list(myhoard_util.build_gtid_ranges(events))
    assert _gtids_of_ranges(ranges) == _gtids_of_events(events)
    assert myhoard_util.partition_sort_and_combine_gtid_ranges(ranges) == {SERVER_UUID: [[1, 2], [5, 7]]}
    assert all(rng["start"] <= rng["end"] for rng in ranges)


@st.composite
def shuffled_run_with_random_timestamps(draw) -> List[myhoard_util.GtidRangeTuple]:
    """Unique GNOs of one server in any order, each with any timestamp, as a source clock can produce"""
    gnos = draw(st.permutations(draw(st.lists(st.integers(1, 60), min_size=1, max_size=40, unique=True))))
    timestamps = draw(st.lists(st.integers(1000, 1010), min_size=len(gnos), max_size=len(gnos)))
    return _timed_events(zip(timestamps, gnos))


@given(shuffled_run_with_random_timestamps())
@settings(max_examples=500, deadline=None)
def test_any_order_and_any_timestamps_give_the_sorted_model(events):
    assert list(myhoard_util.build_gtid_ranges(events)) == _model(events)


@st.composite
def runs_with_repeats(draw) -> List[myhoard_util.GtidRangeTuple]:
    """Up to four runs from two servers, GNOs repeated freely"""
    events: List[myhoard_util.GtidRangeTuple] = []
    for server_uuid in draw(st.lists(st.sampled_from([SERVER_UUID, OTHER_UUID]), min_size=1, max_size=4)):
        events.extend(_events(draw(st.lists(st.integers(1, 30), min_size=1, max_size=30)), server_uuid))
    return events


@given(runs_with_repeats())
@settings(max_examples=500, deadline=None)
def test_repeats_and_server_changes_never_lose_or_invent_a_gtid(events):
    ranges = list(myhoard_util.build_gtid_ranges(events))
    assert _gtids_of_ranges(ranges) == _gtids_of_events(events)
    # A repeated GNO ends a run, so the same GTID can have one timestamp per run and the envelope of a range
    # covers the events of its own run only. Both timestamps still belong to events of the range.
    for rng in ranges:
        assert rng["start"] <= rng["end"]
        assert rng["start_ts"] <= rng["end_ts"]
        timestamps = {
            ts
            for ts, _, server_uuid, gno, _ in events
            if server_uuid == rng["server_uuid"] and rng["start"] <= gno <= rng["end"]
        }
        assert rng["start_ts"] in timestamps and rng["end_ts"] in timestamps


# The reader: which GTIDs of the final file a point in time restore sees


def _write_binlog(path: str, events: Iterable[Tuple[int, int, str, int]], *, with_filler: bool = False) -> None:
    """Write a minimal binlog of GTID events, each (timestamp, server_id, server_uuid, gno), in the given
    file order. With `with_filler` a non GTID event follows every GTID event, as the transaction body does."""
    header_size, gtid_event_code, query_event_code = 19, 33, 2
    with open(path, "wb") as stream:
        stream.write(b"\xfebin")
        position = 4
        for timestamp, server_id, server_uuid, gno in events:
            body = struct.pack("<B16sQ", 1, uuid.UUID(server_uuid).bytes, gno)
            length = header_size + len(body)
            position += length
            stream.write(struct.pack("<IBIIIH", timestamp, gtid_event_code, server_id, length, position, 0) + body)
            if with_filler:
                body = b"BEGIN"
                length = header_size + len(body)
                position += length
                stream.write(struct.pack("<IBIIIH", timestamp, query_event_code, server_id, length, position, 0) + body)


def test_reader_yields_gtid_events_with_their_positions(session_tmpdir):
    path = os.path.join(session_tmpdir().strpath, "binlog.000001")
    _write_binlog(path, [(1000, 1, SERVER_UUID, 1), (1001, 1, SERVER_UUID, 2)], with_filler=True)
    assert list(myhoard_util.read_gtids_from_log(path)) == [
        (1000, 1, SERVER_UUID, 1, 4),
        (1001, 1, SERVER_UUID, 2, 4 + 44 + 24),
    ]
    assert list(myhoard_util.read_gtids_from_log(path, read_until_position=4 + 44 + 24)) == [(1000, 1, SERVER_UUID, 1, 4)]


def test_reader_stops_at_the_target_time_on_in_order_input(session_tmpdir):
    """Events at or after the target time are excluded, everything before it is read"""
    path = os.path.join(session_tmpdir().strpath, "binlog.000002")
    _write_binlog(path, [(1000, 1, SERVER_UUID, 1), (1001, 1, SERVER_UUID, 2), (1002, 1, SERVER_UUID, 3)])
    assert [event[3] for event in myhoard_util.read_gtids_from_log(path, read_until_time=1002)] == [1, 2]
    assert [event[3] for event in myhoard_util.read_gtids_from_log(path, read_until_time=1000)] == []


@pytest.mark.xfail(
    strict=True,
    reason="Known limitation, kept on purpose: read_gtids_from_log returns at the first event at or after the "
    "target time, so a transaction that committed before the target but was written after one that committed "
    "at the target is left out of the restore. Reading on would make MySQL apply the transactions written before "
    "it that committed at or after the target too, since UNTIL SQL_AFTER_GTIDS applies everything up to the last "
    "GTID of its set. A hole before the target was chosen over an overshoot past it.",
)
def test_reader_sees_a_late_transaction_that_committed_before_the_target_time(session_tmpdir):
    """GNO 3 committed at 1001 and was written before GNO 2, which committed at 1000. A restore to 1001 would
    have to apply GNO 2 to be complete. The events of a replica's binlog carry the source's clock, so this
    order exists in a file. See the xfail reason for why the reader is left as it is."""
    path = os.path.join(session_tmpdir().strpath, "binlog.000003")
    _write_binlog(path, [(1000, 1, SERVER_UUID, 1), (1001, 1, SERVER_UUID, 3), (1000, 1, SERVER_UUID, 2)])
    assert sorted(event[3] for event in myhoard_util.read_gtids_from_log(path, read_until_time=1001)) == [1, 2]


# Consumers in the restore coordinator


def _coordinator(session_tmpdir, *, target_time: Optional[int]) -> RestoreCoordinator:
    tmpdir = session_tmpdir().strpath
    return RestoreCoordinator(
        binlog_streams=[{"site": "default", "stream_id": "stream"}],
        download_workers_count=1,
        file_storage_config={},
        free_memory_percentage=80,
        mysql_client_params="-",
        mysql_config_file_name="-",
        mysql_data_directory="/dev/null",
        mysql_relay_log_index_file="/dev/null",
        mysql_relay_log_prefix=os.path.join(tmpdir, "relay"),
        pending_binlogs_state_file=os.path.join(tmpdir, "pending_binlogs.json"),
        rebuild_tables=False,
        restart_mysqld_callback=lambda **kwargs: None,
        rsa_private_key_pem="/dev/null",
        site="default",
        state_file=os.path.join(tmpdir, "restore_state.json"),
        stats=build_statsd_client(),
        stream_id="stream",
        target_time=target_time,
        temp_dir=tmpdir,
    )


def _listed_binlog(remote_index: int, gtid_ranges: List[myhoard_util.GtidRangeDict]) -> Dict[str, Any]:
    """A binlog as the file storage lists it, with the metadata backup_stream wrote when uploading it"""
    return {
        "name": f"default/stream/binlogs/0/{remote_index}_{SERVER_ID}",
        "size": 1000,
        "metadata": myhoard_util.make_fs_metadata(
            {
                "compression_algorithm": "snappy",
                "file_size": 1000,
                "gtid_ranges": gtid_ranges,
                "local_index": remote_index,
                "remote_index": remote_index,
                "server_id": SERVER_ID,
            }
        ),
    }


def _list_binlogs(rc: RestoreCoordinator, listing: List[Dict[str, Any]]):
    # pylint: disable=protected-access
    @contextmanager
    def with_transfer(_storage_config):
        yield Mock(list_iter=Mock(return_value=listing))

    with patch.object(rc.file_storage_pool, "with_transfer", with_transfer):
        binlogs, _, target_time_reached = rc._list_binlogs_in_bucket(0)
    assert binlogs is not None, "listing failed, see the error logged by RestoreCoordinator"
    return [binlog["remote_index"] for binlog in binlogs], target_time_reached


def test_pitr_listing_keeps_a_binlog_whose_first_range_starts_at_the_target_time_but_holds_an_earlier_transaction(
    session_tmpdir,
):
    """GNO 2 committed at 1000 and GNO 1 at 1001. A restore to 1001 must apply GNO 2, so the file must be
    kept. Skipping it also marks the target time reached for the server, which drops every later file."""
    rc = _coordinator(session_tmpdir, target_time=1001)
    kept, _ = _list_binlogs(rc, [_listed_binlog(1, _ranges([2, 1, 3]))])
    assert kept == [1]


def test_pitr_listing_marks_the_target_time_reached_by_the_file_whose_last_transaction_reaches_it(session_tmpdir):
    """GNO 3 was written last, at 1003, after GNO 4 at 1002. A restore to 1003 applies nothing after this
    file, so the listing must report the target time reached, or the coordinator fetches the next files
    and computes the UNTIL clause of the final round from the wrong file.

    This is not a regression: the builder before the fold looked only at the first range in file order
    and misses this case as well. It passes once the range timestamps envelope the folded events."""
    rc = _coordinator(session_tmpdir, target_time=1003)
    kept, target_time_reached = _list_binlogs(rc, [_listed_binlog(1, _ranges([1, 2, 4, 3]))])
    assert kept == [1]
    assert target_time_reached


def test_pitr_listing_skips_every_file_of_a_server_once_one_starts_at_the_target_time(session_tmpdir):
    """The behaviour the skip check exists for: a file whose earliest transaction is at or after the target has
    nothing to apply, and neither has any later file of the same server"""
    rc = _coordinator(session_tmpdir, target_time=1002)
    listing = [
        _listed_binlog(1, _ranges(_timed_events([(1000, 1), (1001, 2)]))),
        _listed_binlog(2, _ranges(_timed_events([(1002, 3), (1003, 4)]))),
        _listed_binlog(3, _ranges(_timed_events([(1004, 5)]))),
    ]
    kept, target_time_reached = _list_binlogs(rc, listing)
    assert kept == [1]
    assert target_time_reached


# Review comment 2 on restore_coordinator.py: the UNTIL clause covers the whole file, the completion check one range


def _gtids(gtid_set: str) -> Set[Tuple[str, int]]:
    return {
        (server_uuid, gno)
        for server_uuid, ranges in myhoard_util.parse_gtid_range_string(gtid_set).items()
        for start, end in ranges
        for gno in range(start, end + 1)
    }


class _FakeCursor:
    """Records every statement and answers GTID_SUBSET against a fixed gtid_executed"""

    def __init__(self, *, gtid_executed: str) -> None:
        self.gtid_executed = gtid_executed
        self.executed: List[Tuple[str, Optional[list]]] = []
        self._result: Optional[Dict[str, Any]] = None

    def execute(self, sql: str, args: Optional[list] = None) -> None:
        self.executed.append((sql, args))
        if sql.startswith("SELECT GTID_SUBSET("):
            assert args is not None
            self._result = {
                "executed": int(_gtids(args[0]) <= _gtids(self.gtid_executed)),
                "gtid_executed": self.gtid_executed,
            }
        else:
            self._result = None

    def fetchone(self) -> Optional[Dict[str, Any]]:
        return self._result

    def statements(self, prefix: str) -> List[str]:
        return [sql for sql, _ in self.executed if sql.startswith(prefix)]


REPLICA_STATUS = {
    "Last_SQL_Error": "",
    "Relay_Log_File": "relay.000002",
    "Replica_SQL_Running": "Yes",
    "Replica_SQL_Running_State": "Reading event from the relay log",
}


def _pending_binlog(remote_index: int, gtid_ranges: List[myhoard_util.GtidRangeDict]) -> PendingBinlogInfo:
    return {
        "adjusted_index": remote_index,
        "adjusted_remote_index": remote_index,
        "compression_algorithm": "snappy",
        "file_size": 1000,
        "gtid_ranges": gtid_ranges,
        "remote_index": remote_index,
        "remote_key": f"default/stream/binlogs/0/{remote_index}_{SERVER_ID}",
        "remote_size": 1000,
    }


@contextmanager
def _mysql_replaced(rc: RestoreCoordinator, cursor: _FakeCursor, final_file_events: List[myhoard_util.GtidRangeTuple]):
    """Run the coordinator against the fake cursor, with the file system and mysqld side effects patched out"""

    @contextmanager
    def mysql_cursor(**_kwargs):
        yield cursor

    with ExitStack() as stack:
        stack.enter_context(patch.object(rc, "_mysql_cursor", mysql_cursor))
        stack.enter_context(patch.object(rc, "_ensure_mysql_server_is_started"))
        stack.enter_context(patch.object(rc, "_generate_updated_relay_log_index"))
        stack.enter_context(patch.object(myhoard.restore_coordinator, "change_replication_source_to"))
        stack.enter_context(
            patch.object(myhoard.restore_coordinator, "read_gtids_from_log", return_value=iter(final_file_events))
        )
        stack.enter_context(patch.object(myhoard.restore_coordinator, "get_replica_status", return_value=REPLICA_STATUS))
        yield


def _apply_round(rc: RestoreCoordinator, binlogs: List[PendingBinlogInfo]) -> None:
    """Queue the binlogs as downloaded, from the second remote index on so the first round file work is skipped"""
    for binlog in binlogs:
        rc.pending_binlog_manager.append(binlog)
    rc.update_state(
        basebackup_info={"binlog_position": 4, "gtid_executed": {}},
        prefetched_binlogs={binlog["remote_key"]: binlog for binlog in binlogs},
    )
    rc.apply_binlogs()


def test_apply_completion_check_waits_for_every_gtid_of_the_final_file(session_tmpdir):
    """The final file holds GNOs 1..100, then 105, then 101, and UNTIL SQL_AFTER_GTIDS covers all of them.
    The completion check must agree with the UNTIL clause: while 105 is applied and 101 is not, the apply
    is not finished. Reporting it finished stops the replica before 101 and then records the whole
    gtid_ranges of the file as executed."""
    # pylint: disable=protected-access
    events = _events(list(range(1, 101)) + [105, 101])
    rc = _coordinator(session_tmpdir, target_time=2000)
    cursor = _FakeCursor(gtid_executed=f"{SERVER_UUID}:1-100:105")
    with _mysql_replaced(rc, cursor, events):
        _apply_round(rc, [_pending_binlog(2, _ranges(events))])
        until_clauses = cursor.statements("START REPLICA SQL_THREAD UNTIL")
        assert len(until_clauses) == 1, cursor.executed
        until_gtids = until_clauses[0].split("SQL_AFTER_GTIDS = ")[1].strip("'")
        assert (SERVER_UUID, 101) in _gtids(until_gtids), until_gtids
        assert rc.state["phase"] == RestoreCoordinator.Phase.waiting_for_apply_to_finish

        finished, _ = rc._check_sql_replica_status()

    assert not finished
    assert not cursor.statements("STOP REPLICA")


def test_apply_completion_check_waits_for_both_servers_of_the_final_file(session_tmpdir):
    """The final file holds a run of one server and then a run of another, as after a node replacement.
    The check must wait for the last GTID of either server, and stop the replica once both are applied."""
    # pylint: disable=protected-access
    events = _events([1, 2, 3], SERVER_UUID) + _events([1, 2], OTHER_UUID, 2)
    rc = _coordinator(session_tmpdir, target_time=2000)
    cursor = _FakeCursor(gtid_executed=f"{SERVER_UUID}:1-3,{OTHER_UUID}:1")
    with _mysql_replaced(rc, cursor, events):
        _apply_round(rc, [_pending_binlog(2, _ranges(events))])
        assert not rc._check_sql_replica_status()[0]
        cursor.gtid_executed = f"{SERVER_UUID}:1-3,{OTHER_UUID}:1-2"
        assert rc._check_sql_replica_status()[0]
    assert cursor.statements("STOP REPLICA") == ["STOP REPLICA"]


def test_apply_without_target_time_waits_for_all_ranges_of_the_last_binlog_with_gtids(session_tmpdir):
    """Without a target time there is no UNTIL clause. The target is the full gtid_ranges of the last binlog of
    the batch that has any, even when a later binlog of the batch is empty."""
    # pylint: disable=protected-access
    with_gtids = _pending_binlog(2, _ranges([1, 2, 4, 3]))
    empty = _pending_binlog(3, [])
    rc = _coordinator(session_tmpdir, target_time=None)
    cursor = _FakeCursor(gtid_executed=f"{SERVER_UUID}:1-3")
    with _mysql_replaced(rc, cursor, []):
        _apply_round(rc, [with_gtids, empty])
        assert cursor.statements("START REPLICA SQL_THREAD") == ["START REPLICA SQL_THREAD"]
        assert rc.state["current_executed_gtid_target"] == with_gtids["gtid_ranges"]
        assert rc.state["current_relay_log_target"] == 4
        # The relay log index guard is satisfied, GTID 4 still is not applied
        with patch.dict(REPLICA_STATUS, {"Relay_Log_File": "relay.000004"}):
            assert not rc._check_sql_replica_status()[0]
            cursor.gtid_executed = f"{SERVER_UUID}:1-4"
            assert rc._check_sql_replica_status()[0]


def test_apply_finalizes_when_nothing_in_the_final_file_precedes_the_target_time(session_tmpdir):
    """Every transaction of the final file is at or after the target time, so the reader returns nothing, no
    replica is started and the restore moves on to finalizing"""
    rc = _coordinator(session_tmpdir, target_time=1000)
    cursor = _FakeCursor(gtid_executed="")
    with _mysql_replaced(rc, cursor, []):
        _apply_round(rc, [_pending_binlog(2, _ranges([1, 2, 3]))])
    assert not cursor.statements("START REPLICA")
    assert rc.state["phase"] == RestoreCoordinator.Phase.finalizing
    assert not rc.state["applying_binlogs"]


def test_completion_check_accepts_the_single_range_target_of_an_older_state_file(session_tmpdir):
    """A restore that was in flight when myhoard was upgraded has one range in current_executed_gtid_target"""
    # pylint: disable=protected-access
    rc = _coordinator(session_tmpdir, target_time=None)
    single_range = _ranges([5, 6, 7])[0]
    rc.state["current_executed_gtid_target"] = single_range  # type: ignore[typeddict-item]
    rc.update_state(current_relay_log_target=None)
    cursor = _FakeCursor(gtid_executed=f"{SERVER_UUID}:1-6")
    with _mysql_replaced(rc, cursor, []):
        assert not rc._check_sql_replica_status()[0]
        cursor.gtid_executed = f"{SERVER_UUID}:1-7"
        assert rc._check_sql_replica_status()[0]
