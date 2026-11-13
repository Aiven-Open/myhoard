# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
"""Tests for the `allow_incremental_only` option of the backup pause, no MySQL needed."""

from __future__ import annotations

from . import build_statsd_client
from dataclasses import dataclass
from myhoard.backup_stream import BackupStream
from myhoard.controller import Controller
from myhoard.web_server import WebServer
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import datetime
import pytest
import threading

pytestmark = [pytest.mark.unittest, pytest.mark.all]

FUTURE = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)).isoformat()
PAST = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).isoformat()
SCHEDULED_TIME = "2023-01-05T03:00:00+00:00"


@dataclass
class Harness:
    """A Controller built without MySQL, plus the mocks standing in for everything it would call"""

    controller: Controller
    mark_backup_requested: MagicMock
    start_new_backup: MagicMock
    update_state: MagicMock
    get_incremental_backup_info: MagicMock
    should_schedule_incremental_backup: MagicMock
    log: MagicMock

    def run_periodic_check(self) -> None:
        # pylint: disable=protected-access
        self.controller._mark_periodic_backup_requested_if_interval_exceeded()

    def run_stream_creation(self) -> None:
        # pylint: disable=protected-access
        self.controller._create_new_backup_stream_if_requested_and_max_streams_not_exceeded()

    def assert_requested(self, *, incremental: bool) -> None:
        self.mark_backup_requested.assert_called_once_with(
            backup_reason=BackupStream.BackupReason.scheduled,
            normalized_backup_time=SCHEDULED_TIME,
            incremental_requested=incremental,
        )

    def assert_not_requested(self) -> None:
        self.mark_backup_requested.assert_not_called()


def build_harness(
    *,
    paused_until: str | None = None,
    incremental_due: bool = True,
    incremental_enabled: bool = True,
    incremental_possible: bool = True,
    behaviors: dict[str, Any] | None = None,
    backup_request: dict[str, Any] | None = None,
) -> Harness:
    mark_backup_requested = MagicMock()
    start_new_backup = MagicMock()
    update_state = MagicMock()
    get_incremental_backup_info = MagicMock(return_value=object() if incremental_possible else None)
    should_schedule_incremental_backup = MagicMock(return_value=incremental_due)
    log = MagicMock()
    controller = object.__new__(Controller)
    # Controller.__init__ needs a MySQL server, only inject what the code under test reads. Instance attributes
    # take precedence over the methods of the same name, which is how the mocks replace them.
    controller.__dict__.update(
        log=log,
        lock=threading.RLock(),
        backup_streams=[],
        backup_settings={
            "backup_interval_minutes": 1440,
            "incremental": {"enabled": incremental_enabled, "full_backup_week_schedule": "mon"},
        },
        state={"paused_backups_until": paused_until, "backup_request": backup_request, **(behaviors or {})},
        state_manager=MagicMock(update_state=update_state),
        _current_normalized_backup_timestamp=MagicMock(return_value=SCHEDULED_TIME),
        _previous_normalized_backup_timestamp=MagicMock(return_value="2023-01-04T03:00:00+00:00"),
        _should_schedule_incremental_backup=should_schedule_incremental_backup,
        get_incremental_backup_info=get_incremental_backup_info,
        mark_backup_requested=mark_backup_requested,
        _start_new_backup=start_new_backup,
    )
    return Harness(
        controller=controller,
        mark_backup_requested=mark_backup_requested,
        start_new_backup=start_new_backup,
        update_state=update_state,
        get_incremental_backup_info=get_incremental_backup_info,
        should_schedule_incremental_backup=should_schedule_incremental_backup,
        log=log,
    )


ALLOW = {"paused_allow_incremental_only": True}


@pytest.mark.parametrize("incremental_due", [True, False])
def test_not_paused_schedules_regardless_of_flag(incremental_due: bool) -> None:
    harness = build_harness(incremental_due=incremental_due, behaviors=ALLOW)
    harness.run_periodic_check()
    harness.assert_requested(incremental=incremental_due)


def test_expired_pause_schedules() -> None:
    harness = build_harness(paused_until=PAST, incremental_due=False)
    harness.run_periodic_check()
    harness.assert_requested(incremental=False)


@pytest.mark.parametrize("incremental_due", [True, False])
def test_paused_default_skips_everything(incremental_due: bool) -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=incremental_due)
    harness.run_periodic_check()
    harness.assert_not_requested()


def test_paused_allow_incremental_only_schedules_incremental() -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=True, behaviors=ALLOW)
    harness.run_periodic_check()
    harness.assert_requested(incremental=True)


def test_paused_allow_incremental_only_skips_full() -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=False, behaviors=ALLOW)
    harness.run_periodic_check()
    harness.assert_not_requested()


def test_paused_allow_incremental_only_without_incremental_enabled_skips_everything() -> None:
    # incremental_due is False whenever the customer did not enable incremental backups
    harness = build_harness(paused_until=FUTURE, incremental_due=False, incremental_enabled=False, behaviors=ALLOW)
    harness.run_periodic_check()
    harness.assert_not_requested()


def test_paused_allow_incremental_only_not_possible_is_skipped_not_full() -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=True, incremental_possible=False, behaviors=ALLOW)
    harness.run_periodic_check()
    harness.assert_not_requested()


def incremental_request(reason: BackupStream.BackupReason) -> dict[str, Any]:
    return {"backup_reason": reason, "normalized_backup_time": SCHEDULED_TIME, "incremental_requested": True}


def full_request(reason: BackupStream.BackupReason) -> dict[str, Any]:
    return {"backup_reason": reason, "normalized_backup_time": SCHEDULED_TIME, "incremental_requested": False}


@pytest.mark.parametrize("incremental", [True, False])
@pytest.mark.parametrize("behaviors", [None, ALLOW], ids=["plain_pause", "flag"])
def test_paused_scheduled_request_is_held_when_not_allowed(incremental: bool, behaviors: dict[str, Any] | None) -> None:
    # A plain pause holds everything, the flag only lets a possible incremental backup through
    request = incremental_request if incremental else full_request
    harness = build_harness(
        paused_until=FUTURE,
        incremental_possible=False,
        behaviors=behaviors,
        backup_request=request(BackupStream.BackupReason.scheduled),
    )
    harness.run_stream_creation()
    harness.start_new_backup.assert_not_called()
    harness.update_state.assert_not_called()  # kept as it is, not dropped


def test_paused_held_request_is_evaluated_once_per_hour(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_700_000_000.0
    monkeypatch.setattr("myhoard.controller.time", MagicMock(time=lambda: now))
    harness = build_harness(
        paused_until=FUTURE,
        incremental_possible=False,
        behaviors=ALLOW,
        backup_request=incremental_request(BackupStream.BackupReason.scheduled),
    )
    for _ in range(3600):
        harness.run_stream_creation()
        now += 1
    assert harness.get_incremental_backup_info.call_count == 1
    assert harness.log.warning.call_count == 1
    harness.run_stream_creation()
    assert harness.get_incremental_backup_info.call_count == 2


def test_paused_scheduled_incremental_request_starts_with_flag_when_possible() -> None:
    harness = build_harness(
        paused_until=FUTURE,
        behaviors=ALLOW,
        backup_request=incremental_request(BackupStream.BackupReason.scheduled),
    )
    harness.run_stream_creation()
    harness.start_new_backup.assert_called_once()
    assert harness.start_new_backup.call_args.kwargs["incremental_backup_info"] is not None


def test_held_scheduled_request_starts_unchanged_once_the_pause_ends() -> None:
    harness = build_harness(
        paused_until=FUTURE, incremental_possible=False, backup_request=full_request(BackupStream.BackupReason.scheduled)
    )
    harness.run_stream_creation()
    harness.start_new_backup.assert_not_called()
    harness.controller.state["paused_backups_until"] = PAST
    harness.run_stream_creation()
    harness.start_new_backup.assert_called_once()
    assert harness.start_new_backup.call_args.kwargs["backup_reason"] == BackupStream.BackupReason.scheduled


@pytest.mark.parametrize("paused_until, replaced", [(FUTURE, True), (None, False)])
def test_manual_request_replaces_scheduled_one_held_by_pause(paused_until: str | None, replaced: bool) -> None:
    harness = build_harness(paused_until=paused_until, backup_request=full_request(BackupStream.BackupReason.scheduled))
    controller = harness.controller
    del controller.__dict__["mark_backup_requested"]  # use the real method
    controller.mark_backup_requested(
        backup_reason=BackupStream.BackupReason.requested, normalized_backup_time=SCHEDULED_TIME
    )
    assert harness.update_state.called is replaced


def test_paused_manual_incremental_request_still_falls_back_to_full() -> None:
    harness = build_harness(
        paused_until=FUTURE,
        incremental_possible=False,
        backup_request=incremental_request(BackupStream.BackupReason.requested),
    )
    harness.run_stream_creation()
    harness.start_new_backup.assert_called_once()
    assert harness.start_new_backup.call_args.kwargs["incremental_backup_info"] is None


def test_not_paused_scheduled_incremental_request_falls_back_to_full() -> None:
    harness = build_harness(
        incremental_possible=False,
        backup_request=incremental_request(BackupStream.BackupReason.scheduled),
    )
    harness.run_stream_creation()
    harness.start_new_backup.assert_called_once()


def test_pause_backups_stores_flag_and_resume_resets_it() -> None:
    harness = build_harness()
    controller = harness.controller
    harness.update_state.side_effect = controller.state.update
    controller.log = MagicMock()
    until = datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc)

    controller.pause_backups(until, allow_incremental_only=True)
    assert controller.state["paused_backups_until"] == until.isoformat()
    assert controller.state["paused_allow_incremental_only"] is True

    # a new pause request replaces the flag, omitting it means the default
    controller.pause_backups(until)
    assert controller.state["paused_allow_incremental_only"] is False

    controller.pause_backups(until, allow_incremental_only=True)
    controller.pause_backups(None)
    assert controller.state["paused_backups_until"] is None
    assert controller.state["paused_allow_incremental_only"] is False


async def test_patch_backup_settings_allow_incremental_only(aiohttp_client) -> None:
    harness = build_harness()
    controller = harness.controller
    harness.update_state.side_effect = controller.state.update
    controller.log = MagicMock()
    server = WebServer(controller=controller, http_address="::1", http_port=-1, stats=build_statsd_client())
    client = await aiohttp_client(server.app)

    response = await client.patch("/backup/settings", json={"paused_until": FUTURE, "allow_incremental_only": True})
    assert response.status == 200
    assert controller.state["paused_backups_until"] == datetime.datetime.fromisoformat(FUTURE).isoformat()
    assert controller.state["paused_allow_incremental_only"] is True

    # an invalid request changes nothing
    for body in (
        {"paused_until": FUTURE, "allow_incremental_only": "yes"},
        {"paused_until": None, "allow_incremental_only": True},
        {"allow_incremental_only": True},
    ):
        response = await client.patch("/backup/settings", json=body)
        assert response.status == 400, body
    assert controller.state["paused_allow_incremental_only"] is True

    response = await client.patch("/backup/settings", json={"paused_until": FUTURE})
    assert response.status == 200
    assert controller.state["paused_allow_incremental_only"] is False

    response = await client.patch("/backup/settings", json={"paused_until": FUTURE, "allow_incremental_only": True})
    response = await client.patch("/backup/settings", json={"paused_until": None})
    assert response.status == 200
    assert controller.state["paused_backups_until"] is None
    assert controller.state["paused_allow_incremental_only"] is False


class _Clock:
    """Fake clock for `myhoard.controller.datetime` and `myhoard.controller.time`"""

    def timestamp(self) -> float:
        return self.current.timestamp()

    def __init__(self, start: datetime.datetime) -> None:
        self.current = start
        clock = self

        class FakeDatetime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):  # type: ignore[override]
                return clock.current

        self.datetime_module = SimpleNamespace(
            datetime=FakeDatetime, timedelta=datetime.timedelta, timezone=datetime.timezone
        )
        self.time_module = MagicMock(time=self.timestamp)


@pytest.mark.parametrize("backup_hour", [3, 13])
@pytest.mark.parametrize("paused", [False, True])
def test_schedule_over_moving_clock_keeps_skipped_full_skipped(
    monkeypatch: pytest.MonkeyPatch, backup_hour: int, paused: bool
) -> None:
    """Runs the real schedule functions over several days: a full backup slot skipped while paused must not come
    back as an incremental backup once the weekday changes"""
    utc = datetime.timezone.utc
    clock = _Clock(datetime.datetime(2023, 1, 7, 12, 0, tzinfo=utc))  # Saturday
    monkeypatch.setattr("myhoard.controller.datetime", clock.datetime_module)
    monkeypatch.setattr("myhoard.controller.time", clock.time_module)

    def stream_at(slot: datetime.datetime) -> Any:
        return MagicMock(
            state={"normalized_backup_time": slot.isoformat(), "backup_reason": BackupStream.BackupReason.scheduled},
            created_at=slot.timestamp(),
        )

    harness = build_harness(
        paused_until=(clock.current + datetime.timedelta(days=10)).isoformat() if paused else None,
        behaviors=ALLOW,
    )
    controller = harness.controller
    for name in (
        "_current_normalized_backup_timestamp",
        "_previous_normalized_backup_timestamp",
        "_should_schedule_incremental_backup",
    ):
        del controller.__dict__[name]  # use the real methods
    controller.backup_settings.update(backup_hour=backup_hour, backup_minute=0)
    controller.backup_settings["incremental"]["full_backup_week_schedule"] = "sun"
    # Last backup: Saturday's slot
    controller.backup_streams.append(stream_at(datetime.datetime(2023, 1, 7, backup_hour, 0, tzinfo=utc)))

    requests: list[tuple[str, str, bool]] = []

    def record(
        *, backup_reason: Any, normalized_backup_time: str, incremental_requested: bool  # pylint: disable=unused-argument
    ) -> None:
        requests.append((clock.current.strftime("%a %H:%M"), normalized_backup_time, incremental_requested))
        controller.backup_streams.append(stream_at(datetime.datetime.fromisoformat(normalized_backup_time)))

    controller.mark_backup_requested = MagicMock(side_effect=record)  # type: ignore[method-assign]

    end = datetime.datetime(2023, 1, 11, 0, 0, tzinfo=utc)  # Wednesday
    while clock.current < end:
        controller._mark_periodic_backup_requested_if_interval_exceeded()  # pylint: disable=protected-access
        clock.current += datetime.timedelta(minutes=30)

    def slot(day: int) -> str:
        return datetime.datetime(2023, 1, day, backup_hour, 0, tzinfo=utc).isoformat()

    monday, tuesday = (r for r in requests if r[1] in (slot(9), slot(10)))
    assert requests[0][1:] == (slot(8), False) if not paused else True
    assert (monday[0], monday[2]) == (f"Mon {backup_hour:02}:00", True)
    assert (tuesday[0], tuesday[2]) == (f"Tue {backup_hour:02}:00", True)
    if paused:
        # Sunday's full slot is skipped for good, nothing is requested before Monday's own slot
        assert [r[1] for r in requests] == [slot(9), slot(10)]
    else:
        assert requests[0][1:] == (slot(8), False)


@pytest.mark.parametrize("incremental_due", [True, False])
def test_paused_skipped_slot_is_evaluated_once_per_hour(monkeypatch: pytest.MonkeyPatch, incremental_due: bool) -> None:
    now = 1_700_000_000.0
    monkeypatch.setattr("myhoard.controller.time", MagicMock(time=lambda: now))
    harness = build_harness(
        paused_until=FUTURE, incremental_due=incremental_due, incremental_possible=False, behaviors=ALLOW
    )

    for _ in range(3600):  # the loop runs every second
        harness.run_periodic_check()
        now += 1
    assert harness.should_schedule_incremental_backup.call_count == 1
    assert harness.log.warning.call_count == 1  # the one skip message of that evaluation
    harness.run_periodic_check()
    assert harness.should_schedule_incremental_backup.call_count == 2
    harness.assert_not_requested()
