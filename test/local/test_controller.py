# Copyright (c) 2023 Aiven, Helsinki, Finland. https://aiven.io/
from __future__ import annotations

from _pytest.logging import LogCaptureFixture
from myhoard.backup_stream import BackupStream
from myhoard.controller import BackupSiteInfo, Controller, sort_completed_backups
from myhoard.restore_coordinator import RestoreCoordinator
from py.path import local as LocalPath
from test import MySQLConfig
from test.helpers.databases import get_table_size, populate_table
from test.helpers.fixtures import create_controller_in_small_disk
from test.helpers.flow_testers import ControllerFlowTester
from typing import Callable, Iterator

import pytest


@pytest.fixture(scope="function", name="empty_controller_in_small_disk")
def fixture_empty_controller_in_small_disk(
    session_tmpdir: Callable[[], LocalPath], mysql_empty: MySQLConfig, default_backup_site: BackupSiteInfo
) -> Iterator[tuple[Controller, MySQLConfig]]:
    with create_controller_in_small_disk(
        session_tmpdir=session_tmpdir, mysql_config=mysql_empty, default_backup_site=default_backup_site
    ) as controller_and_mysql_config:
        yield controller_and_mysql_config


def test_backup_and_restore(
    master_controller: tuple[Controller, MySQLConfig],
    empty_controller: tuple[Controller, MySQLConfig],
) -> None:
    """Test a successful backup and restore."""
    empty_controller[1].connect_options["password"] = master_controller[1].connect_options["password"]
    populate_table(master_controller[1], "test")

    backup_streams = do_backup(controller=master_controller[0])
    do_restore(target_controller=empty_controller[0], backup_streams=backup_streams)

    orig_size = get_table_size(master_controller[1], "test")
    restored_size = get_table_size(empty_controller[1], "test")

    assert orig_size == restored_size


def test_backup_and_restore_fail_on_disk_full(
    master_controller: tuple[Controller, MySQLConfig],
    empty_controller_in_small_disk: tuple[Controller, MySQLConfig],
    caplog: LogCaptureFixture,
) -> None:
    """Test a backup and restore that fails restoring because the disk is full."""
    empty_controller_in_small_disk[1].connect_options["password"] = master_controller[1].connect_options["password"]
    populate_table(master_controller[1], "test")

    backup_streams = do_backup(controller=master_controller[0])
    do_restore(
        target_controller=empty_controller_in_small_disk[0],
        backup_streams=backup_streams,
        caplog=caplog,
        fail_because_disk_full=True,
    )


def do_backup(controller: Controller) -> list[BackupStream]:
    """Trigger a backup and wait for it to finish."""
    flow_tester = ControllerFlowTester(controller)

    controller.switch_to_active_mode()
    controller.start()

    flow_tester.wait_for_streaming_binlogs()

    # Stream backup.
    controller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)

    flow_tester.wait_for_multiple_streams()
    flow_tester.wait_for_streaming_binlogs()
    flow_tester.wait_for_single_stream()

    return controller.backup_streams


def do_restore(
    target_controller: Controller,
    backup_streams: list[BackupStream],
    caplog: LogCaptureFixture | None = None,
    fail_because_disk_full: bool = False,
) -> None:
    """Trigger a restore and wait for it to finish."""
    bs = backup_streams[0]

    # Restore backup into an empty database.
    flow_tester = ControllerFlowTester(target_controller, caplog=caplog)
    target_controller.start()

    try:
        flow_tester.wait_for_fetched_backup(timeout=2)

        target_controller.restore_backup(site=bs.site, stream_id=bs.stream_id)

        if fail_because_disk_full:
            flow_tester.wait_for_disk_full_being_logged()

            # Check that we have backups, but none of them are broken.
            current_backups = sort_completed_backups(target_controller.state["backups"])
            assert current_backups
            assert all(b["broken_at"] is None for b in current_backups)
            assert target_controller.restore_coordinator is not None
            assert target_controller.restore_coordinator.phase is RestoreCoordinator.Phase.failed
        else:
            flow_tester.wait_for_restore_complete()
    finally:
        target_controller.stop()
