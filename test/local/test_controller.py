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


def test_restore_switches_to_previous_backup_when_latest_is_broken(
    master_controller: tuple[Controller, MySQLConfig],
    empty_controller: tuple[Controller, MySQLConfig],
    caplog: LogCaptureFixture,
) -> None:
    """Test that restoration switches to the previous backup when the requested one is broken.

    Builds a full backup followed by two incremental backups. The latest (incremental) backup is
    marked broken only after the restoring controller has already fetched the backup list, so the
    explicit restore request below still goes through; the restore coordinator must then discover
    on its own, while restoring, that the requested backup is broken and fall back to the previous,
    healthy incremental backup instead.
    """
    master, mysql_master = master_controller
    target, mysql_target = empty_controller
    mysql_target.connect_options["password"] = mysql_master.connect_options["password"]

    flow_tester = ControllerFlowTester(master)
    master.switch_to_active_mode()
    master.start()
    flow_tester.wait_for_streaming_binlogs()

    populate_table(mysql_master, "test")
    full_backup = do_backup_round(master, flow_tester, incremental=False)
    assert not full_backup.state["basebackup_info"]["incremental"]

    populate_table(mysql_master, "test")
    previous_backup = do_backup_round(master, flow_tester, incremental=True)
    assert previous_backup.state["basebackup_info"]["incremental"]

    populate_table(mysql_master, "test")
    broken_backup = do_backup_round(master, flow_tester, incremental=True)
    assert broken_backup.state["basebackup_info"]["incremental"]

    target_flow_tester = ControllerFlowTester(target)
    target.start()
    try:
        target_flow_tester.wait_for_fetched_backup(timeout=2)

        # Mark the latest backup as broken only now: the target's cached backup list (fetched
        # above) still considers it healthy, so the restore request below is accepted. The restore
        # coordinator itself must notice that the backup is broken once it starts restoring it.
        master.mark_stream_as_broken(stream_id=broken_backup.stream_id, broken=True)

        target.restore_backup(site=broken_backup.site, stream_id=broken_backup.stream_id)

        # Two basebackups worth of restoring (the broken one is detected before anything is
        # downloaded, then the previous one is restored in full) can take longer than the default.
        target_flow_tester.wait_for_restore_phase(RestoreCoordinator.Phase.completed, timeout=60)
    finally:
        target.stop()

    assert any(
        f"Cannot use backup={broken_backup.stream_id}" in record.message and "marked as broken" in record.message
        for record in caplog.records
    )

    # The restore coordinator must have switched to the previous, non-broken backup.
    assert target.state["restore_options"]["stream_id"] == previous_backup.stream_id

    orig_size = get_table_size(mysql_master, "test")
    restored_size = get_table_size(mysql_target, "test")
    assert orig_size == restored_size


def do_backup_round(controller: Controller, flow_tester: ControllerFlowTester, *, incremental: bool) -> BackupStream:
    """Request one more backup (full or incremental) on an already started, actively backing up controller."""
    controller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested, incremental_requested=incremental)
    flow_tester.wait_for_multiple_streams()
    flow_tester.wait_for_streaming_binlogs()
    flow_tester.wait_for_single_stream()
    return controller.backup_streams[0]


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
    flow_tester = ControllerFlowTester(target_controller)
    target_controller.start()

    try:
        flow_tester.wait_for_fetched_backup(timeout=2)

        target_controller.restore_backup(site=bs.site, stream_id=bs.stream_id)

        if fail_because_disk_full:
            flow_tester.wait_for_restore_phase(RestoreCoordinator.Phase.failed)

            # check if it failed due to full disk
            assert caplog is not None, "caplog is required for checking full disk message."
            assert any(
                "DiskFullError('No space left on device. Cannot complete xbstream-extract!')" in record.message
                for record in caplog.records
            )

            # Check that we have backups, but none of them are broken.
            current_backups = sort_completed_backups(target_controller.state["backups"])
            assert current_backups
            assert all(b["broken_at"] is None for b in current_backups)

        else:
            # Basebackup restore plus binlog apply regularly takes longer than the
            # default 10s timeout on busy CI runners.
            flow_tester.wait_for_restore_phase(RestoreCoordinator.Phase.completed, timeout=40)
    finally:
        target_controller.stop()
