# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
from myhoard.backup_stream import BackupStream
from myhoard.basebackup_monitor import BasebackupMonitor, BasebackupProgress
from myhoard.basebackup_operation import AbortRequested
from myhoard.controller import Controller
from myhoard.errors import XtraBackupError
from socket import gaierror
from unittest.mock import MagicMock

import io
import json
import pytest
import threading

pytestmark = [pytest.mark.unittest, pytest.mark.all]


@pytest.fixture(name="monitor")
def fixture_monitor(tmp_path):
    return BasebackupMonitor(state_file=str(tmp_path / "monitor.json"), stats=MagicMock())


@pytest.fixture(name="clock")
def fixture_clock(mocker):
    now = [1000.0]
    mocker.patch("myhoard.basebackup_monitor.time.monotonic", side_effect=lambda: now[0])
    return now


def begin(monitor, stream_id="stream"):
    return monitor.begin(site="site", stream_id=stream_id, incremental=False)


def finish(monitor, outcome, stream_id="stream"):
    monitor.finish(site="site", stream_id=stream_id, outcome=outcome)


def value(monitor, metric):
    calls = monitor.stats.gauge_int.call_args_list + monitor.stats.gauge_float.call_args_list
    return next(args.args[1] for args in reversed(calls) if args.args[0] == "myhoard.basebackup." + metric)


def published(stream_id="stream", **kwargs):
    return {
        "site": "site",
        "stream_id": stream_id,
        "resumable": True,
        "basebackup_info": {"end_ts": 100.0},
        **kwargs,
    }


def test_progress_only_advances_on_increasing_values(clock):
    progress = BasebackupProgress()
    progress.update(source="upload", value=10)
    progress.sample(clock[0])
    clock[0] += 30
    for size in (0, 5, 10):
        progress.update(source="upload", value=size)
    assert progress.age == 30
    progress.update(source="files", value=100)
    progress.sample(clock[0])
    assert progress.age == 0
    clock[0] += 10
    progress.update(source="upload", value=20)
    progress.sample(clock[0])
    assert progress.age == 0


def test_progress_uses_monotonic_time(monitor, clock, mocker):
    progress = begin(monitor)
    mocker.patch("myhoard.basebackup_monitor.time.time", return_value=-100000)
    clock[0] += 60
    assert progress.age == 60


def test_callbacks_do_not_read_the_clock(mocker):
    progress = BasebackupProgress()
    clock_read = mocker.patch("myhoard.basebackup_monitor.time.monotonic", side_effect=AssertionError("callback clock"))
    for size in range(1000):
        progress.update(source="upload", value=size)
        progress.update(source="files", value=size)
    clock_read.assert_not_called()


def test_sampling_preserves_progress_followed_by_regressing_callbacks(clock):
    progress = BasebackupProgress()
    progress.update(source="upload", value=100)
    progress.update(source="upload", value=20)
    clock[0] += 1
    progress.sample(clock[0])
    assert progress.age == 0
    clock[0] += 10
    for size in (0, 20, 99, 100):
        progress.update(source="upload", value=size)
        progress.sample(clock[0])
    assert progress.age == 10


def test_slow_upload_is_progress_without_a_minimum_byte_threshold(clock):
    progress = BasebackupProgress()
    for size in range(1, 100):
        clock[0] += 1
        progress.update(source="upload", value=size)
        progress.sample(clock[0])
        assert progress.age == 0
    clock[0] += 901
    progress.sample(clock[0])
    assert progress.age == 901


def test_sampling_adds_at_most_one_interval_to_stall_detection(monitor, clock):
    started = clock[0]
    progress = begin(monitor)
    clock[0] += 0.1
    progress.update(source="upload", value=1)
    clock[0] = started + monitor.SAMPLE_INTERVAL
    monitor.sample_progress()
    clock[0] = started + 900.1
    monitor.sample_progress()
    assert 900 - monitor.SAMPLE_INTERVAL <= progress.age <= 900
    clock[0] += monitor.SAMPLE_INTERVAL
    monitor.sample_progress()
    assert progress.age >= 900


def test_recovered_age_is_preserved_until_new_progress_is_sampled(clock):
    progress = BasebackupProgress(initial_age=300)
    clock[0] += 10
    progress.sample(clock[0])
    assert progress.age == 310
    progress.update(source="files", value=1)
    progress.sample(clock[0])
    assert progress.age == 0


def test_concurrent_counter_writers_and_sampler_keep_high_water_marks():
    progress = BasebackupProgress()
    barrier = threading.Barrier(3)

    def write(source):
        barrier.wait(timeout=2)
        for size in range(1, 10001):
            progress.update(source=source, value=size)
            progress.update(source=source, value=size - 1)

    threads = [threading.Thread(target=write, args=(source,)) for source in ("files", "upload")]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=2)
    while any(thread.is_alive() for thread in threads):
        progress.sample(1000)
    for thread in threads:
        thread.join(timeout=2)
    progress.sample(1000)
    assert progress._sampled_values == {"files": 10000, "upload": 10000}  # pylint: disable=protected-access


def test_thread_samples_more_often_than_it_emits_metrics(monitor, mocker):
    progress = begin(monitor)
    monitor.SAMPLE_INTERVAL = 0.01
    monitor.REPORT_INTERVAL = 60
    first_report = threading.Event()
    sampled = threading.Event()
    monitor.stats.gauge_float.side_effect = lambda *_: first_report.set()
    original = progress.sample

    def sample(now):
        original(now)
        if progress._sampled_values.get("upload") == 1:  # pylint: disable=protected-access
            sampled.set()

    mocker.patch.object(progress, "sample", side_effect=sample)
    monitor.start()
    try:
        assert first_report.wait(timeout=2)
        progress.update(source="upload", value=1)
        assert sampled.wait(timeout=2)
        assert len(monitor.stats.gauge_int.call_args_list) == 2
    finally:
        monitor.stop()


@pytest.mark.parametrize("failing_method", ["sample_progress", "report"])
def test_monitor_failure_waits_instead_of_spinning(monitor, mocker, failing_method):
    mocker.patch.object(monitor, failing_method, side_effect=RuntimeError("monitor failure"))

    def wait(timeout):
        assert timeout > 0
        monitor._stop_event.set()  # pylint: disable=protected-access

    mocker.patch.object(monitor._stop_event, "wait", side_effect=wait)  # pylint: disable=protected-access
    monitor.run()
    monitor.stats.unexpected_exception.assert_called_once()


def test_callbacks_and_periodic_reports_do_not_write_state(monitor, mocker):
    progress = begin(monitor)
    write = mocker.spy(monitor.state_manager, "write_state")
    monitor.stats.reset_mock()
    for size in range(1, 1001):
        progress.update(source="upload", value=size)
    monitor.sample_progress()
    monitor.stats.assert_not_called()
    assert not monitor.stats.method_calls
    monitor.report()
    monitor.report()
    write.assert_not_called()


def test_reports_stall_without_any_callbacks(monitor, clock):
    begin(monitor)
    clock[0] += 901
    monitor.report()
    assert value(monitor, "in_progress") == 1
    assert value(monitor, "progress_age_seconds") == 901
    finish(monitor, "success")
    monitor.report()
    assert value(monitor, "in_progress") == 0
    assert value(monitor, "progress_age_seconds") == 0


def test_one_healthy_backup_does_not_hide_another_stall(monitor, clock):
    begin(monitor, "stalled")
    clock[0] += 600
    healthy = begin(monitor, "healthy")
    healthy.update(source="upload", value=100)
    monitor.report()
    assert value(monitor, "progress_age_seconds") == 600
    finish(monitor, "success", "healthy")
    monitor.report()
    assert value(monitor, "in_progress") == 1
    assert value(monitor, "progress_age_seconds") == 600


def test_abort_is_persisted_and_counted_once(monitor):
    begin(monitor)
    finish(monitor, "shutdown")
    finish(monitor, "shutdown")
    monitor.stats.increase.assert_called_once_with(
        "myhoard.backup_aborted", tags={"reason": "shutdown", "incremental": False}
    )
    restarted = BasebackupMonitor(state_file=monitor.state_manager.state_file, stats=MagicMock())
    restarted.reconcile([])
    restarted.report()
    assert value(restarted, "aborts_since_last_success") == 1
    restarted.stats.increase.assert_not_called()


def test_incremental_interruption_is_tagged(monitor):
    monitor.begin(site="site", stream_id="stream", incremental=True)
    finish(monitor, "shutdown")
    monitor.stats.increase.assert_called_once_with(
        "myhoard.backup_aborted", tags={"reason": "shutdown", "incremental": True}
    )


def test_retry_does_not_clear_abort_count(monitor):
    for _ in range(3):
        begin(monitor)
        finish(monitor, "shutdown")
    begin(monitor)
    monitor.report()
    assert value(monitor, "aborts_since_last_success") == 3
    finish(monitor, "success")
    monitor.report()
    assert value(monitor, "aborts_since_last_success") == 0
    assert value(monitor, "last_success_timestamp") > 0


def test_normal_failure_does_not_count_as_restart(monitor):
    begin(monitor)
    finish(monitor, "failed")
    restarted = BasebackupMonitor(state_file=monitor.state_manager.state_file, stats=MagicMock())
    assert not restarted.needs_recovery
    assert restarted.state["aborts_since_last_success"] == 0
    restarted.stats.increase.assert_not_called()


@pytest.mark.parametrize("backups", [[], [published(resumable=False)], [published(broken_at=50)]])
def test_crash_recovery_of_unpublished_attempt(monitor, backups):
    begin(monitor)
    restarted = BasebackupMonitor(state_file=monitor.state_manager.state_file, stats=MagicMock())
    assert restarted.needs_recovery
    restarted.reconcile(backups)
    restarted.reconcile(backups)
    assert not restarted.needs_recovery
    assert restarted.state["aborts_since_last_success"] == 1
    restarted.stats.increase.assert_called_once_with(
        "myhoard.backup_aborted", tags={"reason": "process_restart", "incremental": False}
    )
    again = BasebackupMonitor(state_file=monitor.state_manager.state_file, stats=MagicMock())
    assert not again.needs_recovery


def test_crash_after_publication_is_success_not_abort(monitor):
    begin(monitor)
    finish(monitor, "shutdown")
    begin(monitor)
    restarted = BasebackupMonitor(state_file=monitor.state_manager.state_file, stats=MagicMock())
    restarted.reconcile([published()])
    assert restarted.state["last_success_timestamp"] == 100
    assert restarted.state["aborts_since_last_success"] == 0
    restarted.stats.increase.assert_not_called()


def test_reconciliation_does_not_touch_live_attempt(monitor):
    begin(monitor)
    monitor.reconcile([])
    assert monitor.state["attempts"]
    assert monitor.state["aborts_since_last_success"] == 0


def test_first_deployment_bootstraps_success_age(monitor):
    monitor.reconcile([published()])
    assert monitor.state["last_success_timestamp"] == 100
    monitor.reconcile([published(basebackup_info={"end_ts": 50})])
    assert monitor.state["last_success_timestamp"] == 100


def test_reporting_thread_runs_independently_and_stops_promptly(monitor):
    emitted = threading.Event()
    monitor.stats.gauge_int.side_effect = lambda *_: emitted.set()
    monitor.start()
    try:
        assert emitted.wait(timeout=2)
    finally:
        monitor.stop()
    assert not monitor.is_alive()


def test_reporting_recovers_after_exception(monitor, mocker):
    emitted = threading.Event()
    monitor.REPORT_INTERVAL = 0.01
    original = monitor.report

    def report():
        original()
        emitted.set()

    count = [0]

    def flaky_report():
        count[0] += 1
        if count[0] == 1:
            raise RuntimeError("collector")
        report()

    mocker.patch.object(monitor, "report", side_effect=flaky_report)
    monitor.start()
    try:
        assert emitted.wait(timeout=2)
    finally:
        monitor.stop()
    monitor.stats.unexpected_exception.assert_called_once()


def test_persistence_failure_does_not_fail_backup_and_is_retried(monitor, mocker):
    original_write = monitor.state_manager.write_state
    write = mocker.patch.object(monitor.state_manager, "write_state", side_effect=OSError("disk full"))
    progress = begin(monitor)
    progress.update(source="upload", value=100)
    assert monitor.state["attempts"]
    write.side_effect = original_write
    monitor.report()
    assert write.call_count == 2
    assert not monitor._dirty  # pylint: disable=protected-access
    with open(monitor.state_manager.state_file) as state_file:
        assert json.load(state_file)["attempts"]


def test_duplicate_begin_and_unknown_outcome(monitor):
    begin(monitor)
    with pytest.raises(ValueError, match="already being monitored"):
        begin(monitor)
    with pytest.raises(ValueError, match="Unknown basebackup outcome"):
        finish(monitor, "invalid")


@pytest.fixture(name="stream")
def fixture_stream(tmp_path, monitor, mocker):
    storage = MagicMock()
    storage.calculate_max_unknown_file_size.return_value = 1024
    stream = BackupStream(
        backup_reason=BackupStream.BackupReason.requested,
        basebackup_monitor=monitor,
        file_storage_setup_fn=lambda: storage,
        mode=BackupStream.Mode.active,
        mysql_client_params={},
        mysql_config_file_name="my.cnf",
        mysql_data_directory=str(tmp_path),
        normalized_backup_time=None,
        rsa_public_key_pem=b"unused",
        remote_binlogs_state_file=str(tmp_path / "binlogs.json"),
        server_id=1,
        site="site",
        state_file=str(tmp_path / "stream.json"),
        stats=monitor.stats,
        stream_id="stream",
        temp_dir=str(tmp_path),
        split_size=100,
    )
    stream.file_storage = storage
    stream.state["basebackup_file_metadata"] = {"json": "{}"}
    operation = mocker.patch("myhoard.backup_stream.BasebackupOperation").return_value
    operation.binlog_info = None
    operation.checkpoints_file_content = "checkpoint"
    operation.data_directory_size_start = operation.data_directory_size_end = 100
    operation.incremental_since_checkpoint = None
    operation.lsn_info = None
    operation.number_of_files = 1
    operation.tool_version = "8.4"
    cursor = mocker.patch("myhoard.backup_stream.mysql_cursor").return_value.__enter__.return_value
    cursor.fetchone.return_value = {"gtid_executed": ""}
    mocker.patch("myhoard.backup_stream.rsa_encrypt_bytes", return_value=b"key")
    return stream, operation


def test_stream_finishes_monitoring_only_after_publication(stream, monitor):
    stream, operation = stream

    def publish(key, _data, **_kwargs):
        assert monitor.state["attempts"]
        if key.endswith("basebackup.json"):
            assert stream.active_phase == BackupStream.ActivePhase.basebackup

    stream.file_storage.store_file_from_memory.side_effect = publish
    stream._take_basebackup()  # pylint: disable=protected-access
    assert operation.create_backup.called
    assert stream.active_phase == BackupStream.ActivePhase.binlog_catchup
    assert not monitor.state["attempts"]
    assert monitor.state["last_success_timestamp"] > 0
    assert stream.basebackup_attempt_progress is None


@pytest.mark.parametrize("error", [XtraBackupError("failed"), RuntimeError("bug"), OSError("network"), gaierror("DNS")])
def test_stream_failures_clear_marker_without_abort(stream, monitor, error):
    stream, operation = stream
    operation.create_backup.side_effect = error
    stream._take_basebackup()  # pylint: disable=protected-access
    assert not monitor.state["attempts"]
    assert monitor.state["last_success_timestamp"] == 0
    assert stream.state["basebackup_errors"] == 1
    assert not any(c.args[0] == "myhoard.backup_aborted" for c in monitor.stats.increase.call_args_list)


def test_retry_backoff_does_not_start_an_attempt(stream, monitor, mocker):
    stream, operation = stream
    stream.last_basebackup_attempt = 1000
    stream.state["basebackup_errors"] = 3
    mocker.patch("myhoard.backup_stream.time.monotonic", return_value=1001)
    begin_attempt = mocker.spy(monitor, "begin")
    stream._take_basebackup()  # pylint: disable=protected-access
    begin_attempt.assert_not_called()
    operation.create_backup.assert_not_called()
    assert not monitor.state["attempts"]
    monitor.report()
    assert value(monitor, "in_progress") == 0


def test_constructor_failure_clears_attempt_marker(stream, monitor, mocker):
    stream, _ = stream
    mocker.patch("myhoard.backup_stream.BasebackupOperation", side_effect=OSError("Cannot read configuration"))
    stream._take_basebackup()  # pylint: disable=protected-access
    assert not monitor.state["attempts"]
    assert stream.basebackup_attempt_progress is None
    assert monitor.state["last_success_timestamp"] == 0
    assert monitor.state["aborts_since_last_success"] == 0
    assert stream.state["basebackup_errors"] == 1
    restarted = BasebackupMonitor(state_file=monitor.state_manager.state_file, stats=MagicMock())
    assert not restarted.needs_recovery


def test_stream_shutdown_is_interruption(stream, monitor):
    stream, operation = stream
    operation.create_backup.side_effect = AbortRequested("BackupStream stop requested")
    stream._take_basebackup()  # pylint: disable=protected-access
    assert not monitor.state["attempts"]
    assert monitor.state["aborts_since_last_success"] == 1
    assert stream.state["basebackup_errors"] == 0


def test_stopping_running_stream_records_interruption(stream, monitor):
    stream, operation = stream
    started = threading.Event()
    aborted = threading.Event()

    def create_backup():
        started.set()
        if not aborted.wait(timeout=2):
            raise RuntimeError("Test did not request shutdown")
        raise AbortRequested("BackupStream stop requested")

    operation.create_backup.side_effect = create_backup
    operation.abort.side_effect = lambda _reason: aborted.set()
    stream.start()
    try:
        assert started.wait(timeout=2)
    finally:
        stream.stop()
    assert not stream.is_alive()
    assert monitor.state["aborts_since_last_success"] == 1
    assert not monitor.state["attempts"]


def test_failure_during_shutdown_counts_as_interruption(stream, monitor):
    stream, operation = stream
    stream.is_running = False
    operation.create_backup.side_effect = OSError("closed pipe")
    stream._take_basebackup()  # pylint: disable=protected-access
    assert monitor.state["aborts_since_last_success"] == 1


def test_publication_failure_is_not_success(stream, monitor):
    stream, _ = stream
    stream.file_storage.store_file_from_memory.side_effect = OSError("storage unavailable")
    stream._take_basebackup()  # pylint: disable=protected-access
    assert not monitor.state["attempts"]
    assert monitor.state["last_success_timestamp"] == 0


def test_split_uploads_and_missing_final_callback(stream, monitor, clock):
    stream, _ = stream
    progress = begin(monitor)
    stream.basebackup_attempt_progress = progress
    stream.file_storage.get_file_size.side_effect = [100, 100, 25]
    calls = [0]

    def upload(_key, _stream, *, metadata, upload_progress_fn):
        del metadata
        if calls[0] == 0:
            clock[0] += 10
            upload_progress_fn(90)
            monitor.sample_progress()
            assert progress.age == 0
            clock[0] += 10
            upload_progress_fn(90)  # Duplicate cumulative callback is not progress.
            monitor.sample_progress()
            assert progress.age == 10
        elif calls[0] == 1:
            clock[0] += 10
            upload_progress_fn(10)  # Counter restarted for the second object.
            monitor.sample_progress()
            assert progress.age == 0
        else:
            clock[0] += 10  # No callback for the last object.
        calls[0] += 1

    stream.file_storage.store_file_object.side_effect = upload
    stream._basebackup_stream_handler(io.BytesIO(b"unused"))  # pylint: disable=protected-access
    monitor.sample_progress()
    assert stream.basebackup_bytes_uploaded == 225
    assert progress.age == 0
    assert len(progress._values) == 1  # pylint: disable=protected-access


def test_file_completion_progress(stream, monitor, clock):
    stream, _ = stream
    progress = begin(monitor)
    stream.basebackup_attempt_progress = progress
    clock[0] += 10
    stream._basebackup_progress_callback(processed_original_bytes=100)  # pylint: disable=protected-access
    monitor.sample_progress()
    assert progress.age == 0
    clock[0] += 10
    stream._basebackup_progress_callback(processed_original_bytes=100)  # pylint: disable=protected-access
    monitor.sample_progress()
    assert progress.age == 10


def test_controller_refresh_reconciles_before_removing_state(monitor, mocker):
    begin(monitor)
    restarted = BasebackupMonitor(state_file=monitor.state_manager.state_file, stats=MagicMock())
    controller = Controller.__new__(Controller)
    controller.basebackup_monitor = restarted
    controller.backup_refresh_interval_base = 100000000000
    controller.state = {"backups_fetched_at": 99999999999, "backups": [published()]}
    controller.backup_sites = {}
    controller.seen_basebackup_infos = {}
    controller.site_transfers = {}
    controller.lock = threading.RLock()
    controller.state_manager = MagicMock()
    controller.state["mode"] = Controller.Mode.active
    listing = mocker.patch.object(controller, "get_backup_list", return_value=[])

    def cleanup(_stream_id):
        assert not restarted.needs_recovery
        assert restarted.state["aborts_since_last_success"] == 1

    mocker.patch.object(controller, "_delete_backup_stream_state", side_effect=cleanup)
    controller._refresh_backups_list()  # pylint: disable=protected-access
    listing.assert_called_once()


def test_storage_failure_during_recovery_preserves_pending_attempt(monitor, mocker):
    begin(monitor)
    restarted = BasebackupMonitor(state_file=monitor.state_manager.state_file, stats=MagicMock())
    controller = Controller.__new__(Controller)
    controller.basebackup_monitor = restarted
    controller.backup_refresh_interval_base = 1
    controller.state = {"backups_fetched_at": 0, "backups": [], "mode": Controller.Mode.active}
    controller.backup_sites = {}
    controller.seen_basebackup_infos = {}
    controller.site_transfers = {}
    controller.lock = threading.RLock()
    controller.state_manager = MagicMock()
    listing = mocker.patch.object(controller, "get_backup_list", side_effect=OSError("Storage unavailable"))
    with pytest.raises(OSError, match="Storage unavailable"):
        controller._refresh_backups_list()  # pylint: disable=protected-access
    assert restarted.needs_recovery
    assert restarted.state["aborts_since_last_success"] == 0
    restarted.stats.increase.assert_not_called()
    controller.state_manager.update_state.assert_not_called()
    listing.side_effect = None
    listing.return_value = []
    controller._refresh_backups_list()  # pylint: disable=protected-access
    assert not restarted.needs_recovery
    assert restarted.state["aborts_since_last_success"] == 1


def test_controller_does_not_bootstrap_success_from_recovery_site(monitor, mocker):
    controller = Controller.__new__(Controller)
    controller.basebackup_monitor = monitor
    controller.backup_refresh_interval_base = 1
    controller.state = {"backups_fetched_at": 0, "backups": [], "mode": Controller.Mode.active}
    controller.backup_sites = {"site": {"recovery_only": True}}
    controller.seen_basebackup_infos = {}
    controller.site_transfers = {}
    controller.lock = threading.RLock()
    controller.state_manager = MagicMock()
    mocker.patch.object(controller, "get_backup_list", return_value=[published()])
    controller._refresh_backups_list()  # pylint: disable=protected-access
    assert monitor.state["last_success_timestamp"] == 0


def test_state_is_small_and_contains_no_progress_samples(monitor):
    progress = begin(monitor)
    for size in range(1000):
        progress.update(source="upload", value=size)
    with open(monitor.state_manager.state_file) as state_file:
        state = json.load(state_file)
    assert state["attempts"]["site/stream"]["stream_id"] == "stream"
    assert len(json.dumps(state)) < 500
