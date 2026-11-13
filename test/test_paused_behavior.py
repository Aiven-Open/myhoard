# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
"""Tests for `paused_incremental_behavior` and `paused_full_behavior` backup settings, no MySQL needed."""

from __future__ import annotations

from . import build_statsd_client
from dataclasses import dataclass
from myhoard.backup_stream import BackupStream
from myhoard.controller import Controller
from myhoard.util import PausedFullBehavior, PausedIncrementalBehavior, validate_paused_behavior_settings
from myhoard.web_server import WebServer
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
    settings: dict[str, Any] | None = None,
    backup_request: dict[str, Any] | None = None,
) -> Harness:
    mark_backup_requested = MagicMock()
    start_new_backup = MagicMock()
    update_state = MagicMock()
    controller = object.__new__(Controller)
    # Controller.__init__ needs a MySQL server, only inject what the code under test reads. Instance attributes
    # take precedence over the methods of the same name, which is how the mocks replace them.
    controller.__dict__.update(
        log=MagicMock(),
        lock=threading.RLock(),
        backup_streams=[],
        backup_settings={
            "backup_interval_minutes": 1440,
            "incremental": {"enabled": incremental_enabled, "full_backup_week_schedule": "mon"},
            **(settings or {}),
        },
        state={"paused_backups_until": paused_until, "backup_request": backup_request},
        state_manager=MagicMock(update_state=update_state),
        _current_normalized_backup_timestamp=MagicMock(return_value=SCHEDULED_TIME),
        _previous_normalized_backup_timestamp=MagicMock(return_value="2023-01-04T03:00:00+00:00"),
        _should_schedule_incremental_backup=MagicMock(return_value=incremental_due),
        get_incremental_backup_info=MagicMock(return_value=object() if incremental_possible else None),
        mark_backup_requested=mark_backup_requested,
        _start_new_backup=start_new_backup,
    )
    return Harness(
        controller=controller,
        mark_backup_requested=mark_backup_requested,
        start_new_backup=start_new_backup,
        update_state=update_state,
    )


@pytest.mark.parametrize("incremental_due", [True, False])
def test_not_paused_schedules_regardless_of_behavior(incremental_due: bool) -> None:
    harness = build_harness(
        incremental_due=incremental_due,
        settings={"paused_incremental_behavior": "skip", "paused_full_behavior": "skip"},
    )
    harness.run_periodic_check()
    harness.assert_requested(incremental=incremental_due)


def test_expired_pause_schedules() -> None:
    harness = build_harness(paused_until=PAST, incremental_due=False)
    harness.run_periodic_check()
    harness.assert_requested(incremental=False)


@pytest.mark.parametrize("incremental_due", [True, False])
def test_paused_defaults_skip_everything(incremental_due: bool) -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=incremental_due)
    harness.run_periodic_check()
    harness.assert_not_requested()


def test_paused_incremental_run_schedules_incremental() -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=True, settings={"paused_incremental_behavior": "run"})
    harness.run_periodic_check()
    harness.assert_requested(incremental=True)


def test_paused_incremental_run_leaves_full_skipped() -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=False, settings={"paused_incremental_behavior": "run"})
    harness.run_periodic_check()
    harness.assert_not_requested()


def test_paused_incremental_skip_with_full_behavior_incremental_skips_incremental() -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=True, settings={"paused_full_behavior": "incremental"})
    harness.run_periodic_check()
    harness.assert_not_requested()


def test_paused_full_replaced_by_incremental() -> None:
    harness = build_harness(paused_until=FUTURE, incremental_due=False, settings={"paused_full_behavior": "incremental"})
    harness.run_periodic_check()
    harness.assert_requested(incremental=True)


def test_paused_full_replacement_skipped_when_incremental_not_enabled() -> None:
    harness = build_harness(
        paused_until=FUTURE,
        incremental_due=False,
        incremental_enabled=False,
        settings={"paused_full_behavior": "incremental"},
    )
    harness.run_periodic_check()
    harness.assert_not_requested()


@pytest.mark.parametrize(
    ("incremental_due", "settings"),
    [
        (True, {"paused_incremental_behavior": "run"}),
        (False, {"paused_full_behavior": "incremental"}),
    ],
)
def test_paused_incremental_not_possible_is_skipped_not_full(incremental_due: bool, settings: dict[str, str]) -> None:
    harness = build_harness(
        paused_until=FUTURE, incremental_due=incremental_due, incremental_possible=False, settings=settings
    )
    harness.run_periodic_check()
    harness.assert_not_requested()


def incremental_request(reason: BackupStream.BackupReason) -> dict[str, Any]:
    return {"backup_reason": reason, "normalized_backup_time": SCHEDULED_TIME, "incremental_requested": True}


def test_paused_scheduled_incremental_request_is_dropped_when_not_possible() -> None:
    harness = build_harness(
        paused_until=FUTURE,
        incremental_possible=False,
        backup_request=incremental_request(BackupStream.BackupReason.scheduled),
    )
    harness.run_stream_creation()
    harness.start_new_backup.assert_not_called()
    harness.update_state.assert_called_once_with(backup_request=None)


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


def test_validate_paused_behavior_settings() -> None:
    validate_paused_behavior_settings({})
    validate_paused_behavior_settings({"paused_incremental_behavior": "run", "paused_full_behavior": "incremental"})
    with pytest.raises(ValueError, match="paused_incremental_behavior"):
        validate_paused_behavior_settings({"paused_incremental_behavior": "incremental"})
    with pytest.raises(ValueError, match="paused_full_behavior"):
        validate_paused_behavior_settings({"paused_full_behavior": "run"})


def test_update_paused_behavior_sets_and_resets() -> None:
    controller = build_harness().controller
    controller.update_paused_behavior(incremental=PausedIncrementalBehavior.RUN, full=PausedFullBehavior.INCREMENTAL)
    assert controller.backup_settings["paused_incremental_behavior"] == "run"
    assert controller.backup_settings["paused_full_behavior"] == "incremental"

    controller.update_paused_behavior(incremental=None)
    assert "paused_incremental_behavior" not in controller.backup_settings
    assert controller.backup_settings["paused_full_behavior"] == "incremental"


async def test_patch_backup_settings_paused_behavior(aiohttp_client) -> None:
    controller = build_harness().controller
    server = WebServer(controller=controller, http_address="::1", http_port=-1, stats=build_statsd_client())
    client = await aiohttp_client(server.app)

    response = await client.patch(
        "/backup/settings", json={"paused_incremental_behavior": "run", "paused_full_behavior": "incremental"}
    )
    assert response.status == 200
    assert controller.backup_settings["paused_incremental_behavior"] == "run"
    assert controller.backup_settings["paused_full_behavior"] == "incremental"

    for body in (
        {"paused_incremental_behavior": "incremental"},
        {"paused_full_behavior": "run"},
        {"paused_full_behavior": 1},
    ):
        response = await client.patch("/backup/settings", json=body)
        assert response.status == 400, body
    assert controller.backup_settings["paused_incremental_behavior"] == "run"
    assert controller.backup_settings["paused_full_behavior"] == "incremental"

    response = await client.patch("/backup/settings", json={"paused_incremental_behavior": None})
    assert response.status == 200
    assert "paused_incremental_behavior" not in controller.backup_settings
