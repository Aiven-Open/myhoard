# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
from .state_manager import StateManager
from typing import TYPE_CHECKING, TypedDict

import logging
import threading
import time

if TYPE_CHECKING:
    from .controller import Backup
    from .statsd import StatsClient


class AttemptInfo(TypedDict):
    site: str
    stream_id: str
    incremental: bool
    started_at: float


class MonitorState(TypedDict):
    attempts: dict[str, AttemptInfo]
    aborts_since_last_success: int
    last_success_timestamp: float


class BasebackupProgress:
    """Byte counters for callbacks; only the monitor samples progress and time.

    Keep the small counter lock: storage implementations may invoke callbacks
    concurrently. Retaining high-water marks also preserves progress when a
    decreasing callback arrives before the next sample.
    """

    def __init__(self, *, initial_age: float = 0):
        self._lock = threading.Lock()
        self._values: dict[str, int] = {}
        self._sampled_values: dict[str, int] = {}
        self._last_progress = time.monotonic() - max(0, initial_age)

    def update(self, *, source: str, value: int) -> None:
        with self._lock:
            if value > self._values.get(source, 0):
                self._values[source] = value

    def sample(self, now: float) -> None:
        with self._lock:
            if self._values != self._sampled_values:
                self._sampled_values = self._values.copy()
                self._last_progress = now

    @property
    def age(self) -> float:
        with self._lock:
            return max(0, time.monotonic() - self._last_progress)


class BasebackupMonitor(threading.Thread):
    """Report even when backup callbacks stop; persist only attempt transitions.

    The state belongs to this node. A restarted monitor reconciles its unfinished
    attempts with the controller's first fresh backup listing before cleanup.
    Binlog catchup and streaming are deliberately outside this monitor's scope.
    """

    REPORT_INTERVAL = 15.0
    SAMPLE_INTERVAL = 1.0

    def __init__(self, *, state_file: str, stats: "StatsClient"):
        super().__init__(daemon=True, name="basebackup-monitor")
        self.stats = stats
        self.log = logging.getLogger(self.__class__.__name__)
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._dirty = False
        self.state: MonitorState = {"attempts": {}, "aborts_since_last_success": 0, "last_success_timestamp": 0.0}
        self.state_manager = StateManager(lock=self._lock, state=self.state, state_file=state_file)
        self._pending_recovery = set(self.state["attempts"])
        self._progress = {
            key: BasebackupProgress(initial_age=time.time() - attempt["started_at"])
            for key, attempt in self.state["attempts"].items()
        }

    @property
    def needs_recovery(self) -> bool:
        with self._lock:
            return bool(self._pending_recovery)

    @staticmethod
    def _key(site: str, stream_id: str) -> str:
        return f"{site}/{stream_id}"

    def _persist(self) -> None:
        # A monitoring write failure must not interrupt an otherwise healthy backup.
        # Retry on the next report, while keeping the failure visible in metrics/logs.
        try:
            self.state_manager.write_state()
            self._dirty = False
        except OSError as ex:
            self._dirty = True
            self.log.exception("Failed to persist basebackup monitoring state")
            self.stats.unexpected_exception(ex=ex, where="BasebackupMonitor.persist")

    def begin(self, *, site: str, stream_id: str, incremental: bool) -> BasebackupProgress:
        key = self._key(site, stream_id)
        with self._lock:
            if key in self.state["attempts"]:
                raise ValueError(f"Basebackup attempt {key!r} is already being monitored")
            progress = BasebackupProgress()
            self.state["attempts"][key] = {
                "site": site,
                "stream_id": stream_id,
                "incremental": incremental,
                "started_at": time.time(),
            }
            self._progress[key] = progress
            self._persist()
            return progress

    def finish(self, *, site: str, stream_id: str, outcome: str, completed_at: float | None = None) -> None:
        if outcome not in {"success", "failed", "shutdown", "process_restart"}:
            raise ValueError(f"Unknown basebackup outcome: {outcome!r}")
        key = self._key(site, stream_id)
        with self._lock:
            attempt = self.state["attempts"].pop(key, None)
            if attempt is None:
                return
            self._progress.pop(key, None)
            self._pending_recovery.discard(key)
            if outcome == "success":
                self.state["aborts_since_last_success"] = 0
                self.state["last_success_timestamp"] = max(
                    self.state["last_success_timestamp"], completed_at if completed_at is not None else time.time()
                )
            elif outcome != "failed":
                self.state["aborts_since_last_success"] += 1
            self._persist()
        if outcome not in {"success", "failed"}:
            self.log.warning("Basebackup %s interrupted: %s", key, outcome)
            self.stats.increase("myhoard.backup_aborted", tags={"reason": outcome, "incremental": attempt["incremental"]})

    def reconcile(self, backups: list["Backup"]) -> None:
        """Use an existing authoritative listing, without extra storage requests.

        Publication may have succeeded immediately before a crash. Such a backup
        is successful even if it has not caught up with binlogs yet.
        """
        published = {
            self._key(backup["site"], backup["stream_id"]): backup
            for backup in backups
            if backup["resumable"] and not backup.get("broken_at")
        }
        with self._lock:
            # Bootstrap completion age when deploying monitoring on an existing node.
            if not self.state["last_success_timestamp"] and published:
                self.state["last_success_timestamp"] = max(
                    backup["basebackup_info"]["end_ts"] for backup in published.values()
                )
                self._persist()
            pending = list(self._pending_recovery)
            # Reconcile published attempts first, then interruptions that still need recovery.
            pending.sort(key=lambda key: key not in published)
            for key in pending:
                attempt = self.state["attempts"][key]
                backup = published.get(key)
                self.finish(
                    site=attempt["site"],
                    stream_id=attempt["stream_id"],
                    outcome="success" if backup else "process_restart",
                    completed_at=backup["basebackup_info"]["end_ts"] if backup else None,
                )

    def report(self) -> None:
        with self._lock:
            if self._dirty:
                self._persist()
            progress_age = max((progress.age for progress in self._progress.values()), default=0.0)
            in_progress = int(bool(self._progress))
            aborts = self.state["aborts_since_last_success"]
            last_success = self.state["last_success_timestamp"]
        # Emission does not hold the lifecycle or progress locks.
        self.stats.gauge_int("myhoard.basebackup.in_progress", in_progress)
        self.stats.gauge_float("myhoard.basebackup.progress_age_seconds", progress_age)
        self.stats.gauge_int("myhoard.basebackup.aborts_since_last_success", aborts)
        self.stats.gauge_float("myhoard.basebackup.last_success_timestamp", last_success)
        self.stats.gauge_float("myhoard.basebackup.monitor_timestamp", time.time())

    def sample_progress(self) -> None:
        now = time.monotonic()
        with self._lock:
            for progress in self._progress.values():
                progress.sample(now)

    def run(self) -> None:
        next_report = 0.0
        while not self._stop_event.is_set():
            try:
                self.sample_progress()
                now = time.monotonic()
                if now >= next_report:
                    next_report = now + self.REPORT_INTERVAL
                    self.report()
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Failed to report basebackup monitoring metrics")
                self.stats.unexpected_exception(ex=ex, where="BasebackupMonitor.report")
            until_report = next_report - time.monotonic()
            self._stop_event.wait(min(self.SAMPLE_INTERVAL, until_report) if until_report > 0 else self.SAMPLE_INTERVAL)

    def stop(self) -> None:
        self._stop_event.set()
        if self.ident is not None:
            self.join()
        self.report()
