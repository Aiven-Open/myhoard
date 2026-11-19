from myhoard.controller import Controller
from myhoard.recovery_window import build_recovery_windows, inspect_binlogs
from myhoard.util import file_name_for_basebackup_split, make_fs_metadata
from rohmu import get_transfer
from types import SimpleNamespace
from unittest.mock import Mock

import contextlib
import json
import pytest
import threading

pytestmark = [pytest.mark.unittest, pytest.mark.all]


@pytest.mark.parametrize(
    "indexes, expected_gaps",
    [
        ([], []),
        ([1, 2, 3], []),
        ([2, 3], [{"expected_index": 1, "found_index": 2}]),
        ([1, 3], [{"expected_index": 2, "found_index": 3}]),
    ],
)
def test_inspect_binlogs(indexes, expected_gaps):
    binlogs = [{"remote_index": index, "server_id": 1} for index in reversed(indexes)]
    valid, gaps = inspect_binlogs(binlogs, {1: 1})
    assert [binlog["remote_index"] for binlog in valid] == indexes
    assert gaps == expected_gaps


def test_inspect_binlogs_after_promotion():
    binlogs = [
        {"remote_index": 1, "server_id": 1},
        {"remote_index": 2, "server_id": 1},
        {"remote_index": 3, "server_id": 1},
        {"remote_index": 2, "server_id": 2},
    ]
    valid, gaps = inspect_binlogs(binlogs, {1: 1, 2: 2})
    assert valid == [binlogs[0], binlogs[3]]
    assert not gaps


def make_backup(stream_id, start, first_gtid):
    return {
        "site": "default",
        "stream_id": stream_id,
        "completed_at": start,
        "closed_at": None,
        "broken_at": None,
        "resumable": True,
        "basebackup_info": {"end_ts": start, "gtid_executed": {"uuid": [[1, first_gtid]]}},
    }


def make_binlog(index, start, end, timestamp, *, server_id=1, server_uuid="uuid"):
    return {
        "remote_index": index,
        "server_id": server_id,
        "gtid_ranges": [
            {"server_uuid": server_uuid, "start": start, "end": end, "start_ts": timestamp, "end_ts": timestamp}
        ],
    }


DEFAULT_PROMOTIONS = [{"start_index": 1, "server_id": 1, "promoted_at": 1}]


def inspect_windows(backups, streams, known_binlogs=None, promotions=None):
    storage = Mock()
    storage.list_prefixes.side_effect = lambda prefix: [f"{prefix}/0"]

    def list_iter(prefix):
        stream_id = prefix.split("/")[1]
        if prefix.endswith("/promotions"):
            entries = (promotions or {}).get(stream_id, DEFAULT_PROMOTIONS)
        else:
            entries = streams[stream_id]
        return [{"metadata": make_fs_metadata(entry)} for entry in entries]

    storage.list_iter.side_effect = list_iter
    return build_recovery_windows(backups, {"default": storage}, known_binlogs or {})["default"]


def test_overlapping_windows_reach_latest():
    backups = [make_backup("old", 10, 1), make_backup("new", 20, 2)]
    result = inspect_windows(backups, {"old": [make_binlog(1, 2, 3, 25)], "new": [make_binlog(1, 3, 4, 30)]})
    assert result["window"]["start"] == 10
    assert result["window"]["end"] == 30
    assert result["gaps"] == []


def test_gap_moves_window_to_newer_basebackup():
    backups = [make_backup("old", 10, 1), make_backup("new", 25, 3)]
    streams = {"old": [make_binlog(1, 2, 2, 20), make_binlog(3, 4, 4, 30)], "new": [make_binlog(1, 4, 5, 40)]}
    result = inspect_windows(backups, streams)
    assert result["window"] == {"start": 25, "end": 40, "backups": [{"start": 25, "end": 40, "stream_id": "new"}]}
    assert result["gaps"] == [{"stream_id": "old", "expected_index": 2, "found_index": 3}]


def test_gap_cannot_report_window_through_latest():
    result = inspect_windows([make_backup("old", 10, 1)], {"old": [make_binlog(1, 2, 2, 20), make_binlog(3, 4, 4, 30)]})
    assert result["window"] is None
    assert result["latest_binlog_timestamp"] == 30
    assert result["gaps"]


def test_gtid_gap_despite_consecutive_objects():
    result = inspect_windows([make_backup("old", 10, 1)], {"old": [make_binlog(1, 3, 4, 30)]})
    assert result["window"] is None
    assert result["gaps"][0]["expected_gtid"] == 2


def test_deleted_tail_is_detected_from_known_uploads():
    result = inspect_windows(
        [make_backup("old", 10, 1)],
        {"old": [make_binlog(1, 2, 2, 20)]},
        {("default", "old"): [make_binlog(2, 3, 3, 30)]},
    )
    assert result["window"] is None
    assert result["latest_binlog_timestamp"] == 30
    assert result["gaps"][0]["missing_through_index"] == 2


@pytest.mark.parametrize("broken, incremental", [(True, False), (False, True)])
def test_unusable_basebackup(broken, incremental):
    backup = make_backup("old", 10, 1)
    backup["broken_at"] = 11 if broken else None
    backup["basebackup_info"]["incremental"] = incremental
    result = inspect_windows([backup], {"old": [make_binlog(1, 2, 2, 20)]})
    assert result["window"] is None
    assert result["excluded_backups"] == ["old"]


def test_no_binlogs():
    result = inspect_windows([make_backup("old", 10, 1)], {"old": []})
    assert result["window"] is None
    assert result["latest_binlog_timestamp"] is None


def test_deleted_tail_with_same_timestamp():
    result = inspect_windows(
        [make_backup("old", 10, 1)],
        {"old": [make_binlog(1, 2, 2, 20)]},
        {("default", "old"): [make_binlog(2, 3, 3, 20)]},
    )
    assert result["window"] is None


def test_remote_audit_reads_storage_again(tmp_path):
    storage_config = {"storage_type": "local", "directory": str(tmp_path)}
    storage = get_transfer(storage_config)
    backups = [make_backup("old", 10, 1), make_backup("new", 25, 3)]
    streams = {
        "old": [make_binlog(1, 2, 2, 20), make_binlog(2, 3, 3, 25), make_binlog(3, 4, 4, 30)],
        "new": [make_binlog(1, 4, 5, 40)],
    }
    for backup in backups:
        prefix = f"default/{backup['stream_id']}"
        storage.store_file_from_memory(f"{prefix}/basebackup.xbstream", b"backup")
        storage.store_file_from_memory(f"{prefix}/basebackup.json", json.dumps(backup["basebackup_info"]).encode())
        storage.store_file_from_memory(
            f"{prefix}/completed.json", b"{}", metadata=make_fs_metadata({"completed_at": backup["completed_at"]})
        )
        storage.store_file_from_memory(
            f"{prefix}/promotions/1",
            b"{}",
            metadata=make_fs_metadata({"start_index": 1, "server_id": 1, "promoted_at": 1}),
        )
        for binlog in streams[backup["stream_id"]]:
            storage.store_file_from_memory(
                f"{prefix}/binlogs/0/{binlog['remote_index']}_1", b"binlog", metadata=make_fs_metadata(binlog)
            )
    controller = SimpleNamespace(
        lock=contextlib.nullcontext(),
        backup_sites={"default": {"object_storage": storage_config}},
        backup_streams=[],
        get_backup_list=Controller.get_backup_list,
    )
    result = Controller._run_recovery_window_audit(controller)  # pylint: disable=protected-access
    assert result["sites"]["default"]["window"]["start"] == 10
    storage.delete_key("default/old/binlogs/0/2_1")
    result = Controller._run_recovery_window_audit(controller)  # pylint: disable=protected-access
    site = result["sites"]["default"]
    assert site["window"]["start"] == 25
    assert site["window"]["end"] == 40
    assert site["gaps"] == [{"stream_id": "old", "expected_index": 2, "found_index": 3}]
    assert site["time_since_latest_binlog"] == result["checked_at"] - 40

    storage.store_file_from_memory(
        "default/new/binlogs/2/1000_1", b"binlog", metadata=make_fs_metadata(make_binlog(1000, 6, 6, 50))
    )
    site = Controller._run_recovery_window_audit(controller)["sites"]["default"]  # pylint: disable=protected-access
    assert site["window"] is None
    assert site["latest_binlog_timestamp"] == 50
    assert {"stream_id": "new", "expected_index": 2, "found_index": 1000} in site["gaps"]


def test_incremental_requires_complete_base_chain():
    full = make_backup("full", 10, 1)
    incremental = make_backup("incremental", 25, 3)
    incremental["basebackup_info"].update(incremental=True, required_streams=["full"])
    streams = {"full": [], "incremental": [make_binlog(1, 4, 5, 40)]}
    assert inspect_windows([full, incremental], streams)["window"]["start"] == 25
    full["broken_at"] = 15
    assert inspect_windows([full, incremental], streams)["window"] is None
    assert inspect_windows([incremental], streams)["window"] is None


def test_missing_split_excludes_backup(tmp_path):
    storage = get_transfer({"storage_type": "local", "directory": str(tmp_path)})
    backup = make_backup("split", 10, 1)
    backup["basebackup_info"]["number_of_splits"] = 3
    for index in (1, 3):
        storage.store_file_from_memory(file_name_for_basebackup_split("default/split/basebackup.xbstream", index), b"data")
    storage.store_file_from_memory(
        "default/split/promotions/1",
        b"{}",
        metadata=make_fs_metadata({"start_index": 1, "server_id": 1, "promoted_at": 1}),
    )
    storage.store_file_from_memory(
        "default/split/binlogs/0/1_1", b"data", metadata=make_fs_metadata(make_binlog(1, 2, 3, 20))
    )
    result = build_recovery_windows([backup], {"default": storage}, {})["default"]
    assert result["window"] is None
    assert result["excluded_backups"] == ["split"]


def test_empty_sites():
    result = build_recovery_windows([], {"first": Mock(), "second": Mock()}, {})
    assert set(result) == {"first", "second"}
    assert all(site["window"] is None for site in result.values())


def test_windows_before_gap_are_reported():
    old = make_backup("old", 10, 1)
    old["closed_at"] = 35
    backups = [old, make_backup("new", 50, 5)]
    streams = {
        "old": [make_binlog(1, 2, 2, 20), make_binlog(2, 3, 3, 30)],
        "new": [make_binlog(1, 6, 6, 60), make_binlog(3, 8, 8, 80)],
    }
    result = inspect_windows(backups, streams)
    assert result["window"] is None
    assert [(window["start"], window["end"]) for window in result["windows"]] == [(10, 30), (50, 60)]
    assert result["backup_windows"] == {"old": {"start": 10, "end": 30}, "new": {"start": 50, "end": 60}}
    assert result["gaps"] == [{"stream_id": "new", "expected_index": 2, "found_index": 3}]


def test_excluded_and_incomplete_backups_have_no_backup_window():
    broken = make_backup("broken", 10, 1)
    broken["broken_at"] = 11
    incomplete = make_backup("incomplete", 20, 1)
    incomplete["completed_at"] = None
    streams = {"broken": [make_binlog(1, 2, 2, 20)], "incomplete": [make_binlog(1, 2, 2, 30)]}
    result = inspect_windows([broken, incomplete], streams)
    assert result["backup_windows"] == {"broken": None, "incomplete": None}


def test_stream_error_does_not_fail_other_streams():
    old = make_backup("old", 10, 1)
    old["closed_at"] = 15
    backups = [old, make_backup("new", 20, 2)]
    streams = {"old": [make_binlog(1, 2, 2, 12)], "new": [make_binlog(1, 3, 3, 30)]}
    result = inspect_windows(backups, streams, promotions={"old": []})
    assert result["errors"] == [{"stream_id": "old", "error": "Missing initial promotion for default/old"}]
    assert result["backup_windows"]["old"] is None
    # "old" was closed before the current window ends so it cannot hide newer binlogs
    assert result["window"]["start"] == 20
    assert result["window"]["end"] == 30


def test_error_on_open_stream_invalidates_current_window():
    backups = [make_backup("old", 10, 1), make_backup("new", 20, 2)]
    streams = {"old": [make_binlog(1, 2, 2, 30)], "new": [make_binlog(1, 3, 3, 40)]}
    result = inspect_windows(backups, streams, promotions={"new": []})
    assert result["errors"][0]["stream_id"] == "new"
    assert result["window"] is None
    assert result["windows"][0]["end"] == 30


def test_failover_to_new_server_uuid_is_continuous():
    promotions = {
        "old": [
            {"start_index": 1, "server_id": 1, "promoted_at": 1},
            {"start_index": 2, "server_id": 2, "promoted_at": 25},
        ]
    }
    streams = {
        "old": [
            make_binlog(1, 2, 3, 20),
            # Uploaded by the old primary after the promotion, superseded by the new primary's binlog 2
            make_binlog(2, 4, 9, 99),
            make_binlog(2, 4, 4, 26, server_id=2),
            make_binlog(3, 1, 5, 40, server_id=2, server_uuid="uuid-2"),
        ]
    }
    result = inspect_windows([make_backup("old", 10, 1)], streams, promotions=promotions)
    assert result["gaps"] == []
    assert result["latest_binlog_timestamp"] == 40
    assert result["window"] == {"start": 10, "end": 40, "backups": [{"start": 10, "end": 40, "stream_id": "old"}]}


def test_failover_losing_old_primary_transactions_is_a_gap():
    promotions = {
        "old": [
            {"start_index": 1, "server_id": 1, "promoted_at": 1},
            {"start_index": 2, "server_id": 2, "promoted_at": 25},
        ]
    }
    streams = {
        "old": [
            make_binlog(1, 2, 3, 20),
            # New primary never received GTID 4 from the old primary
            make_binlog(2, 5, 5, 26, server_id=2),
            make_binlog(3, 1, 5, 40, server_id=2, server_uuid="uuid-2"),
        ]
    }
    result = inspect_windows([make_backup("old", 10, 1)], streams, promotions=promotions)
    assert result["window"] is None
    assert result["backup_windows"]["old"] == {"start": 10, "end": 20}
    assert result["gaps"] == [
        {"stream_id": "old", "remote_index": 2, "server_uuid": "uuid", "expected_gtid": 4, "found_gtid": 5}
    ]


def test_failover_new_server_uuid_must_start_from_first_gtid():
    promotions = {
        "old": [
            {"start_index": 1, "server_id": 1, "promoted_at": 1},
            {"start_index": 2, "server_id": 2, "promoted_at": 25},
        ]
    }
    streams = {"old": [make_binlog(1, 2, 3, 20), make_binlog(2, 3, 5, 40, server_id=2, server_uuid="uuid-2")]}
    result = inspect_windows([make_backup("old", 10, 1)], streams, promotions=promotions)
    assert result["gaps"] == [
        {"stream_id": "old", "remote_index": 2, "server_uuid": "uuid-2", "expected_gtid": 1, "found_gtid": 3}
    ]


def make_audit_controller(results):
    controller = SimpleNamespace(
        lock=threading.RLock(),
        recovery_window_audit=None,
        recovery_window_audit_interval=0.001,
        _recovery_window_audit_lock=threading.Lock(),
        _recovery_window_audit_stop=threading.Event(),
        _run_recovery_window_audit=Mock(side_effect=results),
        stats=Mock(),
        log=Mock(),
    )
    controller.audit_recovery_window = lambda: Controller.audit_recovery_window(controller)
    return controller


def make_audit_result(checked_at, window):
    return {
        "checked_at": checked_at,
        "sites": {
            "default": {
                "window": window,
                "backup_windows": {"a": window},
                "gaps": [],
                "errors": [],
                "time_since_latest_binlog": 1,
            }
        },
    }


def test_recovery_window_audit_is_cached():
    first = make_audit_result(100, {"start": 1, "end": 2})
    second = make_audit_result(200, None)
    controller = make_audit_controller([first, second])
    assert Controller.get_backup_recovery_windows(controller) == (None, {})
    assert Controller.get_recovery_window(controller) is first
    assert Controller.get_recovery_window(controller) is first
    assert Controller.get_backup_recovery_windows(controller) == (100, {("default", "a"): {"start": 1, "end": 2}})
    assert Controller.get_recovery_window(controller, refresh=True) is second
    assert Controller.get_backup_recovery_windows(controller) == (200, {("default", "a"): None})
    assert controller._run_recovery_window_audit.call_count == 2  # pylint: disable=protected-access
    controller.stats.gauge_int.assert_any_call("myhoard.pitr.window_available", 0, tags={"site": "default"})


def test_recovery_window_audit_loop_survives_errors():
    result = make_audit_result(100, None)
    controller = make_audit_controller([RuntimeError("storage down"), result])
    original_audit = controller.audit_recovery_window

    def audit_and_stop_on_success():
        audit = original_audit()
        controller._recovery_window_audit_stop.set()  # pylint: disable=protected-access
        return audit

    controller.audit_recovery_window = audit_and_stop_on_success
    Controller._recovery_window_audit_loop(controller)  # pylint: disable=protected-access
    controller.stats.increase.assert_called_once_with("myhoard.pitr.audit_errors")
    assert controller.recovery_window_audit is result
