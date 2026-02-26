# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from __future__ import annotations

from . import build_controller, DataGenerator, get_mysql_config_options, MySQLConfig, wait_for_condition, while_asserts
from .helpers.version import xtrabackup_version_to_string
from myhoard.backup_stream import BackupStream
from myhoard.basebackup_restore_operation import BasebackupRestoreOperation
from myhoard.controller import Backup, BaseBackup, Controller, sort_completed_backups
from myhoard.restore_coordinator import RestoreCoordinator
from myhoard.util import (
    change_replication_source_to,
    get_xtrabackup_version,
    GtidExecuted,
    make_fs_metadata,
    mysql_cursor,
    parse_gtid_range_string,
    parse_version,
    partition_sort_and_combine_gtid_ranges,
)
from rohmu import get_transfer
from typing import Any, Callable, cast, Dict, List, Optional, Set, TypedDict
from unittest.mock import call, MagicMock, patch

import contextlib
import datetime
import os
import pytest
import random
import re
import time

pytestmark = [pytest.mark.unittest, pytest.mark.all]


class CountDict(TypedDict):
    count: int


def test_old_master_has_failed(default_backup_site, master_controller, mysql_empty, session_tmpdir):
    """Create a master and take backup, ensure some binary logs are created. Start new empty server
    and restore that from backup and promote as new master immediate to simulate scenario where old
    master without standbys has failed and is replaced by new server."""
    mcontroller, master = master_controller
    mysql_empty.connect_options["password"] = master.connect_options["password"]

    new_master_controller: Optional[Controller] = None

    master_dg = DataGenerator(connect_info=master.connect_options, make_temp_tables=False)
    try:
        master_dg.start()

        phase_duration = 1
        # Wait some extra in the beginning to give the new thread time to start up
        time.sleep(1 + phase_duration)
        assert master_dg.row_count > 0

        mcontroller.switch_to_active_mode()
        mcontroller.stats = MagicMock()
        mcontroller.start()

        def master_streaming_binlogs():
            assert mcontroller.backup_streams
            assert mcontroller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog
            complete_backups = [backup for backup in mcontroller.state["backups"] if backup["completed_at"]]
            assert complete_backups

        while_asserts(master_streaming_binlogs, timeout=15)

        time.sleep(phase_duration)

        assert mcontroller.backup_streams[0].remote_binlogs

        mcontroller.stop()

        new_master_controller = build_controller(
            Controller,
            default_backup_site=default_backup_site,
            mysql_config=mysql_empty,
            session_tmpdir=session_tmpdir,
        )
        new_master_controller.binlog_purge_settings["min_binlog_age_before_purge"] = 1
        new_master_controller.binlog_purge_settings["purge_interval"] = 0.1
        new_master_controller.start()

        wait_for_condition(lambda: new_master_controller.state["backups_fetched_at"] != 0, timeout=2)
        backup = new_master_controller.state["backups"][0]
        new_master_controller.restore_backup(site=backup["site"], stream_id=backup["stream_id"])

        def restoration_is_complete():
            assert new_master_controller
            return new_master_controller.restore_coordinator and new_master_controller.restore_coordinator.is_complete()

        wait_for_condition(restoration_is_complete, timeout=30, description="Restoration was not completed in time")

        # Ensure old master manages to send some more binary logs now that new master has finished
        # restoring backup. Because new master isn't connected to old one it won't receive these via
        # replication but it should download and apply them from file storage
        master_dg.stop()
        # Need to re-create the controller because a controller that has been stopped once cannot be
        # started again
        mcontroller = build_controller(
            Controller,
            default_backup_site=default_backup_site,
            mysql_config=master,
            session_tmpdir=session_tmpdir,
            state_dir=mcontroller.state_dir,
            temp_dir=mcontroller.temp_dir,
        )
        mcontroller.start()

        binlogs = set()
        with mysql_cursor(**master.connect_options) as cursor:
            cursor.execute("SHOW BINARY LOGS")
            for binlog in cursor.fetchall():
                binlogs.add(binlog["Log_name"])
            cursor.execute("FLUSH BINARY LOGS")
            cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
            old_master_gtid_executed = cursor.fetchone()["gtid_executed"]

        def has_uploaded_all():
            assert mcontroller.backup_streams
            remote_binlog_names = {binlog["file_name"] for binlog in mcontroller.backup_streams[0].remote_binlogs}
            # Binlogs are uploaded in order, just checking that most recent one is uploaded should be fine
            assert max(remote_binlog_names) == max(binlogs)

        while_asserts(has_uploaded_all, timeout=15)
        mcontroller.stop()

        # Promote new master. It should end up applying everything from master
        new_master_controller.switch_to_active_mode()
        # Wait for backup promotion steps to complete
        wait_for_condition(lambda: new_master_controller.mode == Controller.Mode.active, timeout=15)

        def new_master_has_all_data():
            with mysql_cursor(**mysql_empty.connect_options) as cursor:
                cursor.execute(
                    "SELECT GTID_SUBSET(%s, @@GLOBAL.gtid_executed) AS executed, @@GLOBAL.gtid_executed AS gtid_executed",
                    [old_master_gtid_executed],
                )
                result = cursor.fetchone()
                new_master_gtid_executed = result["gtid_executed"]
                assert result["executed"], f"{old_master_gtid_executed} not subset of {new_master_gtid_executed}"

        while_asserts(new_master_has_all_data, timeout=15)
    finally:
        mcontroller.stop()
        master_dg.stop()
        if new_master_controller:
            new_master_controller.stop()


def test_force_promote(default_backup_site, master_controller, mysql_empty, session_tmpdir):
    """Create a master and take backup, create large table without primary key, delete large number
    of rows from it (which is very slow to replicate with row based replication). Start new empty server
    and restore that from backup and force promote as new master while binary logs are still being
    applied (simulating a scenario where old master has failed but binary logs cannot be applied in
    reasonable amount of time and some data loss is preferable over very long wait)."""
    mcontroller, master = master_controller
    mysql_empty.connect_options["password"] = master.connect_options["password"]

    new_master_controller: Optional[Controller] = None

    try:
        mcontroller.switch_to_active_mode()
        mcontroller.stats = MagicMock()
        mcontroller.start()

        def master_streaming_binlogs():
            assert mcontroller.backup_streams
            assert mcontroller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog
            complete_backups = [backup for backup in mcontroller.state["backups"] if backup["completed_at"]]
            assert complete_backups

        while_asserts(master_streaming_binlogs, timeout=15)

        # Create table with large number of rows and no primary key; updated and deletes from this
        # are very slow to replicate
        with mysql_cursor(**master.connect_options) as cursor:
            cursor.execute("CREATE TABLE large_no_pk (id INTEGER);")
            iteration_entries = 5000
            cursor.execute(f"SET cte_max_recursion_depth = {iteration_entries}")
            current_index = 0
            batches = 40
            while current_index < batches * iteration_entries:
                end = current_index + iteration_entries
                cursor.execute(
                    f"""
                    INSERT INTO large_no_pk (id)
                       SELECT sq.value + {current_index}
                       FROM (WITH RECURSIVE nums AS (
                                SELECT 1 AS value UNION ALL SELECT value + 1 AS value
                                    FROM nums WHERE nums.value < {iteration_entries})
                                SELECT * FROM nums) sq
                    """
                )
                current_index = end
            cursor.execute("COMMIT")
            cursor.execute("DELETE FROM large_no_pk WHERE id = 1")
            cursor.execute("COMMIT")
            max_id = 0
            for index in range(batches - 4):
                max_id = current_index - (index + 1) * iteration_entries
                cursor.execute("DELETE FROM large_no_pk WHERE id > %s", max_id)
                cursor.execute("COMMIT")
                cursor.execute("FLUSH BINARY LOGS")
            assert max_id == 20000
            cursor.execute("SHOW BINARY LOGS")
            wait_for_index = max(int(binlog["Log_name"].split(".")[-1]) for binlog in cursor.fetchall())
            cursor.execute("FLUSH BINARY LOGS")

        wait_for_condition(lambda: mcontroller.is_log_backed_up(log_index=wait_for_index), timeout=10)

        new_master_controller = build_controller(
            Controller,
            default_backup_site=default_backup_site,
            mysql_config=mysql_empty,
            session_tmpdir=session_tmpdir,
        )
        new_master_controller.binlog_purge_settings["min_binlog_age_before_purge"] = 1
        new_master_controller.binlog_purge_settings["purge_interval"] = 0.1
        new_master_controller.start()

        wait_for_condition(lambda: new_master_controller.state["backups_fetched_at"] != 0, timeout=2)
        backup = new_master_controller.state["backups"][0]
        new_master_controller.restore_backup(site=backup["site"], stream_id=backup["stream_id"])

        def applying_binlogs():
            assert new_master_controller
            return (
                new_master_controller.restore_coordinator
                and new_master_controller.restore_coordinator.phase == RestoreCoordinator.Phase.waiting_for_apply_to_finish
            )

        wait_for_condition(applying_binlogs, timeout=15)
        # Wait a bit to ensure our individual row deletion has been applied so that we can verify
        # binary logs were applied partially but we just didn't get to the end because of forced promotion
        time.sleep(2)
        wait_for_condition(applying_binlogs, timeout=2)

        new_master_controller.switch_to_active_mode(force=True)
        wait_for_condition(lambda: new_master_controller.mode == Controller.Mode.active, timeout=15)

        with mysql_cursor(**mysql_empty.connect_options) as cursor:
            cursor.execute("SELECT COUNT(*) AS count FROM large_no_pk WHERE id = 1")
            result = cursor.fetchone()
            # This particular row is expected to be deleted
            assert result["count"] == 0
            cursor.execute("SELECT COUNT(*) AS count FROM large_no_pk WHERE id > 20000")
            result = cursor.fetchone()
            # We tried deleting anything with id above > 20000 but this should not have completed
            # because it was so slow that force promotion took place first
            assert result["count"] > 0
    finally:
        mcontroller.stop()
        if new_master_controller:
            new_master_controller.stop()


def test_promoted_node_does_not_resume_streams_for_backups_initiated_by_old_master(
    master_controller,
    mysql_empty,
    standby1_controller,
):
    """Create a master and one standby, simulate a case where the old master can still complete a backup
    after the promotion process of the standby was triggered, and ensure that the new master does not
    resume streams for the backups that got initiated by the old master after promotion."""
    m_controller, master = master_controller
    s1_controller, standby1 = standby1_controller

    mysql_empty.connect_options["password"] = master.connect_options["password"]
    data_generator = DataGenerator(connect_info=master.connect_options, make_temp_tables=False)

    try:
        data_generator.start()

        m_controller.switch_to_active_mode()
        m_controller.start()

        s1_controller.switch_to_observe_mode()
        s1_controller.start()

        def streaming_binlogs():
            assert m_controller.backup_streams
            assert all(stream.active_phase == BackupStream.ActivePhase.binlog for stream in m_controller.backup_streams)

        while_asserts(streaming_binlogs, timeout=10)

        m_controller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)

        def all_backups_completed(controller, expected_bkps):
            backups = controller.state["backups"]
            assert len(backups) == expected_bkps
            assert all(backup["completed_at"] is not None for backup in backups)

        while_asserts(lambda: all_backups_completed(controller=m_controller, expected_bkps=2), timeout=30)

        # Simulate a case where the old/leaving master creates a new
        # backup after the promotion process of the standby was triggered.
        with mysql_cursor(**standby1.connect_options) as cursor:
            cursor.execute("STOP REPLICA IO_THREAD")
        s1_controller.switch_to_active_mode()

        m_controller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)
        s1_controller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)

        while_asserts(lambda: all_backups_completed(controller=m_controller, expected_bkps=4), timeout=30)
        while_asserts(lambda: all_backups_completed(controller=s1_controller, expected_bkps=4), timeout=30)

        # Wait for both master nodes to see the observe stream for the backup initiated by the other master.
        def all_streams_are_visible(controller, expected_streams):
            assert len(controller.backup_streams) == expected_streams

        while_asserts(lambda: all_streams_are_visible(controller=m_controller, expected_streams=2), timeout=30)
        while_asserts(lambda: all_streams_are_visible(controller=s1_controller, expected_streams=2), timeout=30)

        # Wait for both master nodes to close the observe stream for the backup initiated by the other master.
        def all_streams_are_closed(controller):
            # When all observe streams are closed, we expect to be left
            # with only one non-closed stream, which is the one the node owns.
            non_closed_streams = [
                stream.stream_id for stream in controller.backup_streams if not stream.state["closed_info"]
            ]
            assert len(non_closed_streams) == 1
            assert non_closed_streams == controller.state["owned_stream_ids"]
            assert len(controller.backup_streams) == 1

        while_asserts(lambda: all_streams_are_closed(controller=m_controller), timeout=30)
        while_asserts(lambda: all_streams_are_closed(controller=s1_controller), timeout=30)
    finally:
        data_generator.stop()
        m_controller.stop()
        s1_controller.stop()


def create_fake_state_files(controller: Controller) -> List[str]:
    # pylint: disable=protected-access
    state_file_name = controller._state_file_from_stream_id("1234")
    # pylint: disable=protected-access
    remote_binlogs_state_file_name = controller._remote_binlogs_state_file_from_stream_id("1234")
    with open(state_file_name, "w") as state_file:
        state_file.write(".")
    with open(remote_binlogs_state_file_name, "w") as remote_binlogs_state_file:
        remote_binlogs_state_file.write(".")
    return [state_file_name, remote_binlogs_state_file_name]


def test_backup_state_from_removed_backup_is_removed(default_backup_site, mysql_empty, session_tmpdir):
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    fake_file_names = create_fake_state_files(controller)
    controller.state["backups"] = [
        {
            "basebackup_info": {"end_ts": 0.0},
            "closed_at": None,
            "completed_at": None,
            "recovery_site": False,
            "stream_id": "1234",
            "resumable": False,
            "site": "default",
        }
    ]
    controller._refresh_backups_list()  # pylint: disable=protected-access
    for file_name in fake_file_names:
        assert not os.path.exists(file_name)


def test_backup_state_from_removed_site_is_removed(default_backup_site, mysql_empty, session_tmpdir):
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    fake_file_names = create_fake_state_files(controller)
    controller.state["backups"] = [
        Backup(
            basebackup_info=BaseBackup(end_ts=0.0),
            closed_at=None,
            completed_at=None,
            recovery_site=False,
            stream_id="1234",
            resumable=False,
            site="not_default_site",
        )
    ]
    controller._refresh_backups_list()  # pylint: disable=protected-access
    for file_name in fake_file_names:
        assert not os.path.exists(file_name)


@pytest.mark.skip(reason="Flaky test. Needs to be verified before re-enabling.")
def test_3_node_service_failover_and_restore(
    default_backup_site,
    master_controller,
    mysql_empty,
    session_tmpdir,
    standby1_controller,
    standby2_controller,
):
    """Create master and two standbys, emulate master failure, check that all promotion related logic works
    as expected, create new standby from backup and ensure it ends up in correct state"""
    mcontroller, master = master_controller
    s1controller, standby1 = standby1_controller
    s2controller, standby2 = standby2_controller
    s3controller: List[Controller] = []

    # Empty server will be initialized from backup that has been created from master so it'll use the same password
    # (normally all servers should be restored from same backup but we're not simulating that here now)
    mysql_empty.connect_options["password"] = master.connect_options["password"]

    with mysql_cursor(**master.connect_options) as cursor:
        cursor.execute("SELECT @@GLOBAL.server_uuid AS server_uuid")
        original_server_uuid = cursor.fetchone()["server_uuid"]

    # Set unknown replication state to all controllers so that they won't purge binary logs
    for controller in [mcontroller, s1controller, s2controller]:
        controller.state_manager.update_state(replication_state={"s1": {}})
        controller.binlog_purge_settings["min_binlog_age_before_purge"] = 1
        controller.binlog_purge_settings["purge_interval"] = 0.1

    master_dg = DataGenerator(connect_info=master.connect_options, make_temp_tables=False)
    try:
        master_dg.start()

        phase_duration = 1
        # Wait some extra in the beginning to give the new thread time to start up
        time.sleep(1 + phase_duration)
        assert master_dg.row_count > 0

        mcontroller.switch_to_active_mode()
        s1controller.switch_to_observe_mode()
        s2controller.switch_to_observe_mode()
        mcontroller.stats = MagicMock()
        mcontroller.start()
        s1controller.start()
        s2controller.start()

        last_gtid_executeds = [None, None]

        def streams_available_master_streaming_binlogs(backup_count):
            assert mcontroller.backup_streams
            # Flush binlogs on standbys so that we get to test that new master doesn't upload binlog that
            # only has GTIDs that have already been backed up
            for idx, standby in enumerate([standby1, standby2]):
                with mysql_cursor(**standby.connect_options) as cursor:
                    cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
                    gtid_executed = cursor.fetchone()["gtid_executed"]
                    if gtid_executed != last_gtid_executeds[idx]:
                        cursor.execute("FLUSH BINARY LOGS")
                        last_gtid_executeds[idx] = gtid_executed
            if backup_count > 1:
                # Do some flushes on master to ensure there are simultaneous binlog uploads and existing ones get reused
                with mysql_cursor(**master.connect_options) as cursor:
                    foo_suffix = str(time.time()).replace(".", "_")
                    cursor.execute(f"CREATE TABLE foo_{foo_suffix} (id INTEGER)")
                    cursor.execute("COMMIT")
                    cursor.execute("FLUSH BINARY LOGS")
            assert s1controller.backup_streams
            assert s2controller.backup_streams
            assert mcontroller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog
            complete_backups = [backup for backup in mcontroller.state["backups"] if backup["completed_at"]]
            assert len(complete_backups) == backup_count

        while_asserts(lambda: streams_available_master_streaming_binlogs(1), timeout=30)

        # Take another backup so that we get to test that existing uploads are reused (remote copied)
        mcontroller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)
        while_asserts(lambda: streams_available_master_streaming_binlogs(2), timeout=30)
        mcontroller.stats.increase.assert_any_call("myhoard.binlog.remote_copy")
        mcontroller.stats.increase.assert_any_call("myhoard.binlog.upload")

        for _ in range(5):
            time.sleep(phase_duration / 5)
            for standby in [standby1, standby2]:
                with mysql_cursor(**standby.connect_options) as cursor:
                    cursor.execute("FLUSH BINARY LOGS")

        assert mcontroller.backup_streams[0].remote_binlogs
        assert s1controller.backup_streams[0].remote_binlogs
        assert s2controller.backup_streams[0].remote_binlogs

        assert s1controller.backup_streams[0].state["pending_binlogs"]
        assert s2controller.backup_streams[0].state["pending_binlogs"]

        # No binlogs have been removed from disk at this point in time (because replication state says it's not safe)
        assert mcontroller.binlog_scanner.binlogs[0]["local_index"] == 1
        assert s1controller.binlog_scanner.binlogs[0]["local_index"] == 1
        assert s2controller.binlog_scanner.binlogs[0]["local_index"] == 1

        # Do what would happen during normal promotion; all standbys stop reading from master to ensure they can
        # get to a consistent state, then we pick the standby that is furthest in replication as the new master.
        # Note that we're not stopping master or data generation to master here to ensure rogue master case works.
        with mysql_cursor(**standby1.connect_options) as cursor1:
            with mysql_cursor(**standby2.connect_options) as cursor2:
                cursor1.execute("STOP REPLICA IO_THREAD")
                cursor2.execute("STOP REPLICA IO_THREAD")

                # Wait for SQL threads to apply any relay logs that got downloaded from master
                def relay_log_applied():
                    for cursor in [cursor1, cursor2]:
                        cursor.execute("SHOW REPLICA STATUS")
                        status = cursor.fetchone()["Replica_SQL_Running_State"]
                        assert re.match("(Slave|Replica) has read all relay log; waiting for more updates", status)

                while_asserts(relay_log_applied, timeout=30)
                cursor1.execute("STOP REPLICA SQL_THREAD")
                cursor2.execute("STOP REPLICA SQL_THREAD")

                # Pick whichever standby got furthest in replication as new master
                cursor1.execute("SELECT @@GLOBAL.gtid_executed AS executed")
                executed1 = cursor1.fetchone()["executed"]
                cursor2.execute("SELECT @@GLOBAL.gtid_executed AS executed")
                executed2 = cursor2.fetchone()["executed"]
                latest1 = int(executed1.split("-")[-1])
                latest2 = int(executed2.split("-")[-1])
                new_master = standby2 if latest2 > latest1 else standby1
                new_mcontroller = s2controller if latest2 > latest1 else s1controller
                new_master_cursor = cursor2 if latest2 > latest1 else cursor1
                standby_cursor = cursor1 if latest2 > latest1 else cursor2

                time.sleep(phase_duration)

                def build_and_initialize_controller():
                    state_dir = s3controller[0].state_dir if s3controller else None
                    temp_dir = s3controller[0].temp_dir if s3controller else None
                    new_controller = build_controller(
                        Controller,
                        default_backup_site=default_backup_site,
                        mysql_config=mysql_empty,
                        session_tmpdir=session_tmpdir,
                        state_dir=state_dir,
                        temp_dir=temp_dir,
                    )
                    new_controller.state_manager.update_state(replication_state={"s1": {}})
                    new_controller.binlog_purge_settings["min_binlog_age_before_purge"] = 1
                    new_controller.binlog_purge_settings["purge_interval"] = 0.1
                    new_controller.start()
                    return new_controller

                s3controller = [build_and_initialize_controller()]
                wait_for_condition(lambda: s3controller[0].state["backups_fetched_at"] != 0, timeout=2)
                backup = s3controller[0].state["backups"][0]
                s3controller[0].restore_backup(site=backup["site"], stream_id=backup["stream_id"])

                master_options = {
                    "SOURCE_AUTO_POSITION": 1,
                    "SOURCE_CONNECT_RETRY": 0.1,
                    "SOURCE_HOST": "127.0.0.1",
                    "SOURCE_PASSWORD": new_master.password,
                    "SOURCE_PORT": new_master.port,
                    "SOURCE_SSL": 0,
                    "SOURCE_USER": master.user,
                }
                new_mcontroller.switch_to_active_mode()
                change_replication_source_to(cursor=standby_cursor, options=master_options)
                standby_cursor.execute("START REPLICA IO_THREAD, SQL_THREAD")

                # Wait for backup promotion steps to complete
                wait_for_condition(lambda: new_mcontroller.mode == Controller.Mode.active, timeout=30)

                # pylint: disable=protected-access
                promotions = new_mcontroller.backup_streams[0]._get_promotions(ignore_own_promotion=False)
                assert len(promotions) == 2

                new_master_cursor.execute("SET @@GLOBAL.read_only = 0")

                new_master_dg = DataGenerator(
                    connect_info=new_master.connect_options,
                    index_offset=master_dg.row_count + 1,
                    make_temp_tables=False,
                )
                new_master_dg.start()

                start_time = time.monotonic()
                wait_increased: List[bool] = []
                restart_times: List[float] = []

                def restore_complete():
                    # Re-create the restore coordinator a couple of times during restoration
                    # to ensure that works without problems
                    if len(restart_times) < 3 and random.random() < 0.25 and s3controller[0].is_safe_to_reload():
                        s3controller[0].stop()
                        s3controller[0] = build_and_initialize_controller()
                        restart_times.append(time.monotonic())

                    # Stop generating data for the old master after a while to reduce disk IO pressure so
                    # that standby can make better progress catching up with master
                    if time.monotonic() - start_time > 15:
                        master_dg.stop()
                    # Make data generation slower after a while to ensure standby can catch up
                    if time.monotonic() - start_time > 30 and not wait_increased:
                        new_master_dg.basic_wait = new_master_dg.basic_wait * 2
                        wait_increased.append(True)
                    new_master_cursor.execute("FLUSH BINARY LOGS")
                    return s3controller[0].restore_coordinator and s3controller[0].restore_coordinator.is_complete()

                # Wait for replacement server to finish restoring basebackup and whatever binlogs were available
                # when basebackup restoration finished
                wait_for_condition(restore_complete, timeout=120)

                with mysql_cursor(**mysql_empty.connect_options) as standby3_cursor:
                    change_replication_source_to(cursor=standby3_cursor, options=master_options)
                    standby3_cursor.execute("START REPLICA IO_THREAD, SQL_THREAD")
                    s3controller[0].switch_to_observe_mode()

                    time.sleep(phase_duration)
                    master_dg.stop()
                    time.sleep(phase_duration)
                    new_master_dg.stop()

                    # Ensure all servers have data they should have
                    new_master_cursor.execute("FLUSH BINARY LOGS")

                    def all_nodes_have_same_gtid_executed():
                        new_master_cursor.execute("SELECT @@GLOBAL.gtid_executed AS executed")
                        new_master_executed = new_master_cursor.fetchone()["executed"]
                        # There must be GTIDs with exactly two different server UUIDs
                        assert len(new_master_executed.split(",")) == 2

                        standby_cursor.execute("SELECT @@GLOBAL.gtid_executed AS executed")
                        assert new_master_executed == standby_cursor.fetchone()["executed"]
                        standby3_cursor.execute("SELECT @@GLOBAL.gtid_executed AS executed")
                        assert new_master_executed == standby3_cursor.fetchone()["executed"]

                    while_asserts(all_nodes_have_same_gtid_executed, timeout=60)

                # New master must have uploaded something. What exactly depends on timing and is hard to verify
                # but check the remote binlogs and ensure there's at most one file which has any duplication in
                # GTID ranges
                remote_binlogs = new_mcontroller.backup_streams[0].remote_binlogs
                own_indexes = [
                    binlog["local_index"] for binlog in remote_binlogs if binlog["server_id"] == new_mcontroller.server_id
                ]
                assert own_indexes

                duplicate_count = 0
                all_old_master_ranges: List[List[List[int]]] = []
                for binlog in remote_binlogs:
                    ranges = partition_sort_and_combine_gtid_ranges(binlog["gtid_ranges"])
                    old_master_ranges = ranges.get(original_server_uuid)
                    if not old_master_ranges:
                        continue
                    current_range_start = old_master_ranges[0][0]
                    for range_tuples in all_old_master_ranges:
                        last_range_end = range_tuples[-1][-1]
                        if current_range_start <= last_range_end:
                            print(
                                "Current binlog",
                                binlog,
                                "has range start that predates one of the seen ranges:",
                                range_tuples,
                            )
                            duplicate_count += 1
                            # Only count one duplicate range per file. The file that gets created after promotion might
                            # well have multiple duplicate ranges (which is expected behavior)
                            break
                    all_old_master_ranges.append(old_master_ranges)

                assert all_old_master_ranges
                assert duplicate_count <= 1

                # Set correct replication states for all controllers. Should result in all local binlogs getting purged
                # because everything has been backed up and replicated
                replication_state = {}
                for server_name, controller in [["s1", s1controller], ["s2", s2controller], ["s3", s3controller[0]]]:
                    with mysql_cursor(**controller.mysql_client_params) as cursor:
                        cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
                        gtid_executed = parse_gtid_range_string(cursor.fetchone()["gtid_executed"])
                        replication_state[server_name] = gtid_executed
                for controller in [s1controller, s2controller, s3controller[0]]:
                    controller.state_manager.update_state(replication_state=replication_state)

                # Set GTID_NEXT on standby to cause new GTID entry getting created even though there are no data
                # changes. This should not interfere with binlog removals.
                with mysql_cursor(**s3controller[0].mysql_client_params) as cursor:
                    cursor.execute("SELECT @@GLOBAL.server_uuid AS server_uuid")
                    server_uuid = cursor.fetchone()["server_uuid"]
                    cursor.execute(f"SET @@SESSION.GTID_NEXT = '{server_uuid}:1'")
                    cursor.execute("FLUSH BINARY LOGS")
                    cursor.execute(f"SET @@SESSION.GTID_NEXT = '{server_uuid}:2'")
                    cursor.execute("FLUSH BINARY LOGS")
                    time.sleep(0.5)

                def all_binlogs_purged():
                    for controller in [s1controller, s2controller, s3controller[0]]:
                        if controller.mode == Controller.Mode.active:
                            # For master we may have some binlogs because list of binlogs may end with binlogs that have
                            # no GTID ranges, which cannot be safely cleaned up. But some binlogs must have been flushed
                            binlogs = controller.binlog_scanner.binlogs
                            assert not binlogs or binlogs[0]["local_index"] > 1
                        else:
                            assert not controller.binlog_scanner.binlogs
                            # All pending binlogs must have been removed as well because they're already backed up
                            assert not controller.backup_streams[0].state["pending_binlogs"]

                while_asserts(all_binlogs_purged, timeout=10)
    finally:
        master_dg.stop()
        if new_master_dg:
            new_master_dg.stop()
        if s3controller:
            s3controller[0].stop()

    for controller in [mcontroller, s1controller, s2controller, s3controller[0]]:
        assert controller.state["errors"] == 0
        assert controller.backup_streams[0].state["backup_errors"] == 0
        assert controller.backup_streams[0].state["remote_read_errors"] == 0
        assert controller.backup_streams[0].state["remote_write_errors"] == 0


@pytest.mark.parametrize(
    "standby_fixture_name, expect_remote_copy",
    [
        ("standby1_controller", True),
        ("standby1_controller_cross_site", False),
    ],
    ids=["same_site", "cross_site"],
)
def test_remote_copy_behavior(
    request,
    master_controller,
    standby_fixture_name,
    expect_remote_copy,
):
    """Verify remote-copy behavior depending on whether backup streams share the same site.

    When both streams target the same site, the second stream should use remote-copy
    instead of uploading from local disk.

    When streams target different sites, remote copies are forbidden and
    the second stream must upload from local disk. This is because in real-life
    sites would represents prefixes on cloud object storage buckets. When sites are
    different, there is no way to make sure they are on the same cloud provider or
    not.

    Scenario:
    - Master creates a backup on the 'default' site.
    - Standby observes master's backup, then is promoted to active mode.
    - A new backup is requested, creating a second stream.
    - Both streams see the same local binlog indexes. When stream A uploads a binlog,
      the reference is passed to stream B. Depending on whether they share the same
      site, stream B either does a remote-copy or uploads from local disk.
    """
    mcontroller, master = master_controller
    s1controller, standby1 = request.getfixturevalue(standby_fixture_name)

    # Set unknown replication state so that binary logs won't be purged
    mcontroller.state_manager.update_state(replication_state={"s1": {}})
    mcontroller.binlog_purge_settings["min_binlog_age_before_purge"] = 1
    mcontroller.binlog_purge_settings["purge_interval"] = 0.1

    master_dg = DataGenerator(connect_info=master.connect_options, make_temp_tables=False)
    try:
        # Phase 1: Master creates initial backup on 'default' site
        master_dg.start()
        time.sleep(1)
        assert master_dg.row_count > 0

        mcontroller.switch_to_active_mode()
        mcontroller.start()

        def master_streaming_binlogs():
            assert mcontroller.backup_streams
            assert mcontroller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog
            complete_backups = [b for b in mcontroller.state["backups"] if b["completed_at"]]
            assert len(complete_backups) >= 1

        while_asserts(master_streaming_binlogs, timeout=30)

        # Phase 2: Standby observes, then promotes
        # Stop slave threads (required for observe -> promote transition)
        with mysql_cursor(**standby1.connect_options) as cursor:
            cursor.execute("STOP REPLICA")

        s1controller.switch_to_observe_mode()
        s1controller.start()

        # Wait for standby to discover the master's backup on 'default' site
        def standby_discovered_backup():
            assert s1controller.state["backups_fetched_at"] != 0
            assert len(s1controller.state["backups"]) >= 1

        while_asserts(standby_discovered_backup, timeout=15)

        # Promote standby to active mode
        s1controller.switch_to_active_mode()

        # Wait for promotion to complete (controller mode becomes active)
        def standby_is_active():
            assert s1controller.mode == Controller.Mode.active

        while_asserts(standby_is_active, timeout=15)

        # Replace stats with MagicMock to track remote_copy vs upload calls
        s1controller.stats = MagicMock()

        # Request a new backup — creates a stream on the standby's upload site
        s1controller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)

        # Turn off read_only so the standby can generate binlogs
        with mysql_cursor(**standby1.connect_options) as cursor:
            cursor.execute("SET GLOBAL read_only = 0")

        # Generate data and flush binlogs so both streams have binlogs to upload
        standby_dg = DataGenerator(connect_info=standby1.connect_options, make_temp_tables=False)
        standby_dg.start()

        try:
            # Wait for the new backup to complete its basebackup phase and start uploading binlogs.
            # During binlog_catchup of the new stream, both streams upload binlogs simultaneously,
            # which is the window where remote-copy optimization or cross-site guard is exercised.
            def standby_has_two_completed_backups():
                # Generate some flushes to create binlogs for both streams to upload
                with mysql_cursor(**standby1.connect_options) as cursor:
                    foo_suffix = str(time.time()).replace(".", "_")
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS foo_{foo_suffix} (id INTEGER)")
                    cursor.execute("COMMIT")
                    cursor.execute("FLUSH BINARY LOGS")
                complete_backups = [b for b in s1controller.state["backups"] if b["completed_at"]]
                assert len(complete_backups) >= 2

            while_asserts(standby_has_two_completed_backups, timeout=30)

            # At this point the new backup has completed and both streams have uploaded binlogs.
            if expect_remote_copy:
                # Same site: verify that binlog uploads happened via remote copy
                s1controller.stats.increase.assert_any_call("myhoard.binlog.remote_copy")
            else:
                # Cross site: verify uploads happened from local disk but NO remote copies
                s1controller.stats.increase.assert_any_call("myhoard.binlog.upload")
                remote_copy_calls = [
                    c for c in s1controller.stats.increase.call_args_list if c == call("myhoard.binlog.remote_copy")
                ]
                assert (
                    [] == remote_copy_calls
                ), f"Expected no remote copies due to cross-site guard, but found {len(remote_copy_calls)} remote copy calls"
        finally:
            standby_dg.stop()
    finally:
        master_dg.stop()
        mcontroller.stop()
        s1controller.stop()

    assert s1controller.state["errors"] == 0


def test_empty_server_backup_and_restore(
    default_backup_site,
    master_controller,
    mysql_empty,
    session_tmpdir,
):
    mcontroller, master = master_controller
    s3controller = None

    mysql_empty.connect_options["password"] = master.connect_options["password"]

    try:
        mcontroller.switch_to_active_mode()
        mcontroller.start()

        def streaming_binlogs(controller):
            assert controller.backup_streams
            assert len(controller.backup_streams) == 1
            assert controller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog

        while_asserts(lambda: streaming_binlogs(mcontroller), timeout=15)

        mcontroller.stop()

        s3controller = build_controller(
            Controller,
            default_backup_site=default_backup_site,
            mysql_config=mysql_empty,
            session_tmpdir=session_tmpdir,
        )
        # Change backup schedule. Should not result in creation of new backup on new server because
        # previous one is recent enough
        s3controller.backup_settings["backup_minute"] = (s3controller.backup_settings["backup_minute"] + 1) % 60
        s3controller.start()
        wait_for_condition(lambda: s3controller.state["backups_fetched_at"] != 0, timeout=2)
        backup = s3controller.state["backups"][0]
        s3controller.restore_backup(site=backup["site"], stream_id=backup["stream_id"])

        wait_for_condition(
            lambda: s3controller.restore_coordinator and s3controller.restore_coordinator.is_complete(),
            timeout=20,
        )

        with mysql_cursor(**mysql_empty.connect_options) as cursor:
            cursor.execute("SELECT 1 AS result")
            assert cursor.fetchone()["result"] == 1

        s3controller.switch_to_active_mode()

        while_asserts(lambda: streaming_binlogs(s3controller), timeout=15)
        # No new backup created yet because not sufficient amount of time elapsed since previous one
        assert len(s3controller.state["backups"]) == 1

        original_stream_id = s3controller.backup_streams[0].stream_id
        # 3 seconds
        s3controller.backup_settings["backup_interval_minutes"] = 0.05
        # Give enough time for new backup stream to be started
        time.sleep(0.5)
        while_asserts(lambda: streaming_binlogs(s3controller), timeout=15)
        assert len(s3controller.state["backups"]) == 2
        new_stream_id = s3controller.backup_streams[0].stream_id
        assert new_stream_id != original_stream_id
    finally:
        if s3controller:
            s3controller.stop()


def test_extend_binlog_stream_list(default_backup_site, session_tmpdir):
    backups = MagicMock()

    class DummyController(Controller):
        def extend_binlog_stream_list(self):
            self._extend_binlog_stream_list()

        def _refresh_backups_list(self, force_refresh: bool = False):
            self.state["backups"] = backups()

    name = "dummyserver"
    test_base_dir = os.path.abspath(os.path.join(session_tmpdir().strpath, name))
    config_path = os.path.join(test_base_dir, "etc")
    state_dir = os.path.abspath(os.path.join(session_tmpdir().strpath, "state"))
    temp_dir = os.path.abspath(os.path.join(session_tmpdir().strpath, "temp"))
    controller = build_controller(
        DummyController,
        default_backup_site=default_backup_site,
        mysql_config=MySQLConfig(
            config_options=get_mysql_config_options(
                config_path=config_path, name=name, server_id=1, test_base_dir=test_base_dir
            ),
            config_name=os.path.join(config_path, "my.cnf"),
            connect_options={},
            server_id=1,
        ),
        session_tmpdir=session_tmpdir,
        state_dir=state_dir,
        temp_dir=temp_dir,
    )
    rc = MagicMock()
    controller.restore_coordinator = rc

    # Cannot add backups, extend_binlog_stream_list does nothing
    rc.can_add_binlog_streams.return_value = False
    controller.extend_binlog_stream_list()
    backups.assert_not_called()

    # Can add backups but backup being restored is not the last one, does not try to look up new backups
    rc.can_add_binlog_streams.return_value = True
    controller.state["backups"] = [
        {
            "completed_at": 2.0,
            "site": "a",
            "stream_id": "2",
            "basebackup_info": {"end_ts": 1.0},
            "closed_at": 1.0,
            "recovery_site": False,
            "resumable": True,
        },
        {
            "completed_at": 1.0,
            "site": "a",
            "stream_id": "1",
            "basebackup_info": {"end_ts": 1.0},
            "closed_at": 1.0,
            "recovery_site": False,
            "resumable": True,
        },
    ]
    rc.binlog_streams = [
        {"site": "a", "stream_id": "1"},
    ]
    controller.extend_binlog_stream_list()
    backups.assert_not_called()

    # Can add backups and restoring last backup, looks up new backups but does nothing because no new ones are found
    rc.binlog_streams = [
        {"site": "a", "stream_id": "2"},
    ]
    rc.stream_id = "2"
    backups.return_value = [
        {"completed_at": 3.0, "site": "a", "stream_id": "2"},
        {"completed_at": 2.0, "site": "a", "stream_id": "1"},
        {"completed_at": 1.0, "site": "a", "stream_id": "0"},
    ]
    controller.extend_binlog_stream_list()
    backups.assert_called()
    rc.add_new_binlog_streams.assert_not_called()

    # Can add backups and restoring last backup, new backup is found and added to list of binlog streams to restore
    backups.return_value = [
        {"completed_at": 3.0, "site": "a", "stream_id": "3"},
        {"completed_at": 2.0, "site": "a", "stream_id": "2"},
        {"completed_at": 1.0, "site": "a", "stream_id": "1"},
    ]
    rc.add_new_binlog_streams.return_value = True
    controller.state["restore_options"] = {"binlog_streams": rc.binlog_streams, "foo": "abc"}
    controller.extend_binlog_stream_list()
    backups.assert_called()
    rc.add_new_binlog_streams.assert_called_with([{"site": "a", "stream_id": "3"}])
    assert controller.state["restore_options"] == {
        "binlog_streams": [
            {"site": "a", "stream_id": "2"},
            {"site": "a", "stream_id": "3"},
        ],
        "foo": "abc",
    }


def test_multiple_backup_management(master_controller):
    mcontroller, master = master_controller
    # Backup every 3 seconds
    mcontroller.backup_settings["backup_interval_minutes"] = 0.05
    # Never delete backups if we don't have at least 2 no matter how old they are
    mcontroller.backup_settings["backup_count_min"] = 2
    # Delete backups if there are more than this even if the backup to delete is newer than max age
    mcontroller.backup_settings["backup_count_max"] = 20
    # Max age 12 seconds (3-5 backups at any given time)
    mcontroller.backup_settings["backup_age_days_max"] = 1 / 24 / 60 / 5

    mcontroller.switch_to_active_mode()
    mcontroller.start()

    seen_backups = set()
    highest_backup_count = 0
    last_flush = [time.monotonic()]

    def maybe_flush_binlog():
        if time.monotonic() - last_flush[0] > 0.2:
            with mysql_cursor(**master.connect_options) as cursor:
                cursor.execute("FLUSH BINARY LOGS")
            last_flush[0] = time.monotonic()

    # Wait for 35 seconds and ensure backup count stays between 3 and 4 the whole time (once 3 has been reached)
    start_time = time.monotonic()
    while time.monotonic() - start_time < 35:
        maybe_flush_binlog()
        for backup in mcontroller.state["backups"]:
            seen_backups.add(backup["stream_id"])
        completed_backups = [backup for backup in mcontroller.state["backups"] if backup["completed_at"]]
        if len(completed_backups) > highest_backup_count:
            highest_backup_count = len(completed_backups)
        if highest_backup_count >= 3:
            # For very brief moments there could be 6 backups
            assert 3 <= len(completed_backups) <= 6
        time.sleep(0.1)

    assert highest_backup_count >= 3

    # We waited for 35 seconds altogether and backup interval is 3 seconds. There should be somewhere between
    # 8 and 13 backups depending on timing and how long taking backups takes
    assert 8 <= len(seen_backups) <= 13


def test_manual_backup_creation(master_controller):
    mcontroller = master_controller[0]
    # Never delete backups if we don't have at least 2 no matter how old they are
    mcontroller.backup_settings["backup_count_min"] = 2
    # Delete backups if there are more than this even if the backup to delete is newer than max age
    mcontroller.backup_settings["backup_count_max"] = 5

    mcontroller.switch_to_active_mode()
    mcontroller.start()

    def streaming_binlogs():
        assert mcontroller.backup_streams
        assert mcontroller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog

    while_asserts(streaming_binlogs, timeout=10)

    start_time = time.monotonic()
    seen_backups: Set[str] = set()
    # Create up to 10 backups so that we have enough to verify deleting backups when max count is exceeded works
    while time.monotonic() - start_time < 60 and len(seen_backups) < 10:
        current_backups = set(backup["stream_id"] for backup in mcontroller.state["backups"] if backup["completed_at"])
        if current_backups - seen_backups:
            mcontroller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)
            seen_backups.update(current_backups)
        time.sleep(0.1)

    assert len(seen_backups) >= 10
    time.sleep(0.1)
    current_backups = set(backup["stream_id"] for backup in mcontroller.state["backups"] if backup["completed_at"])
    assert len(current_backups) == 5


def test_automatic_old_backup_recovery(default_backup_site, master_controller, mysql_empty, session_tmpdir):
    mcontroller, master = master_controller

    # Empty server will be initialized from backup that has been created from master so it'll use the same password
    # (normally all servers should be restored from same backup but we're not simulating that here now)
    mysql_empty.connect_options["password"] = master.connect_options["password"]

    mcontroller.switch_to_active_mode()
    mcontroller.start()

    def streaming_binlogs():
        assert mcontroller.backup_streams
        assert all(bs.active_phase == BackupStream.ActivePhase.binlog for bs in mcontroller.backup_streams), [
            (s.name, s.active_phase) for s in mcontroller.backup_streams
        ]

    while_asserts(streaming_binlogs, timeout=10)

    # Write some data to database that predates second backup
    with mysql_cursor(**master.connect_options) as cursor:
        cursor.execute("CREATE TABLE foo (id INTEGER)")
        cursor.execute("INSERT INTO foo VALUES (1)")
        cursor.execute("COMMIT")
        cursor.execute("FLUSH BINARY LOGS")

    mcontroller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)

    def has_multiple_streams():
        assert len(mcontroller.backup_streams) > 1

    while_asserts(has_multiple_streams, timeout=10)
    while_asserts(streaming_binlogs, timeout=10)

    def has_single_stream():
        assert len(mcontroller.backup_streams) == 1

    while_asserts(has_single_stream, timeout=10)

    # Insert something that is only included in the second backup
    with mysql_cursor(**master.connect_options) as cursor:
        cursor.execute("INSERT INTO foo VALUES (2)")
        cursor.execute("COMMIT")
        cursor.execute("FLUSH BINARY LOGS")

    # Corrupt second basebackup just uploading dummy bytes should do it
    bs = mcontroller.backup_streams[0]
    basebackup_name = f"{bs.site}/{bs.stream_id}/basebackup.xbstream"
    bs.file_storage.store_file_from_memory(basebackup_name, b"abc")

    # Restore last backup to empty server. This should initially fail because basebackup is
    # broken but succeed because it then proceeds restoring the earlier backup
    new_controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    new_controller.stats = MagicMock()
    new_controller.start()
    try:
        wait_for_condition(lambda: new_controller.state["backups_fetched_at"] != 0, timeout=2)
        new_controller.restore_backup(site=bs.site, stream_id=bs.stream_id)

        def restore_complete():
            return new_controller.restore_coordinator and new_controller.restore_coordinator.is_complete()

        wait_for_condition(restore_complete, timeout=40)
        new_controller.stats.increase.assert_any_call("myhoard.restore_errors", tags={"ex": "Exception"})
        new_controller.stats.increase.assert_any_call("myhoard.basebackup_broken")
    finally:
        new_controller.stop()

    # Check we have all the expected data available
    with mysql_cursor(**mysql_empty.connect_options) as cursor:
        cursor.execute("SELECT id FROM foo")
        results = cursor.fetchall()
        assert len(results) == 2
        assert {result["id"] for result in results} == {1, 2}


def test_new_binlog_stream_while_restoring(
    default_backup_site,
    master_controller,
    mysql_empty,
    session_tmpdir,
):
    mcontroller, master = master_controller
    s3controller = None

    mysql_empty.connect_options["password"] = master.connect_options["password"]

    try:
        mcontroller.switch_to_active_mode()
        mcontroller.start()

        # Write some data that gets included in the first backup
        with mysql_cursor(**master.connect_options) as cursor:
            cursor.execute("CREATE TABLE foo (id INTEGER)")
            cursor.execute("INSERT INTO foo VALUES (1)")
            cursor.execute("COMMIT")
            cursor.execute("FLUSH BINARY LOGS")

        def streaming_binlogs(controller):
            assert controller.backup_streams
            assert len(controller.backup_streams) == 1
            assert controller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog

        while_asserts(lambda: streaming_binlogs(mcontroller), timeout=15)
        stream_id = mcontroller.backup_streams[0].stream_id

        s3controller = build_controller(
            Controller,
            default_backup_site=default_backup_site,
            mysql_config=mysql_empty,
            session_tmpdir=session_tmpdir,
        )

        binlog_uploaded = False

        @contextlib.contextmanager
        def timing_manager(block_name, tags=None):  # pylint: disable=unused-argument
            if block_name == "myhoard.basebackup_restore.xtrabackup_move":
                while not binlog_uploaded:
                    time.sleep(0.1)
            yield

        s3controller.stats.timing_manager = timing_manager
        s3controller.start()
        wait_for_condition(lambda: s3controller.state["backups_fetched_at"] != 0, timeout=2)
        backup = s3controller.state["backups"][0]
        # Start restoring the only backup we have at the moment
        s3controller.restore_backup(site=backup["site"], stream_id=backup["stream_id"])

        # Start creating new backup
        mcontroller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)

        # Wait until second backup completes
        wait_for_condition(
            lambda: len(mcontroller.backup_streams) == 1 and mcontroller.backup_streams[0].stream_id != stream_id,
            timeout=20,
        )

        # Write something that gets included in the second backup
        with mysql_cursor(**master.connect_options) as cursor:
            cursor.execute("INSERT INTO foo VALUES (2)")
            cursor.execute("COMMIT")
            cursor.execute("FLUSH BINARY LOGS")

        # Ensure the binlog has been processed and uploaded
        time.sleep(1)

        # Backup restoration shouldn't have completed by now because we're blocking before one of
        # the basebackup restore operations
        assert s3controller.restore_coordinator
        assert not s3controller.restore_coordinator.is_complete()

        binlog_uploaded = True

        wait_for_condition(
            lambda: s3controller.restore_coordinator and s3controller.restore_coordinator.is_complete(), timeout=30
        )

        # Restored data should contain changes from first backup that we originally restored plus the binlog
        # from second backup that was created while the first one was restoring
        with mysql_cursor(**mysql_empty.connect_options) as cursor:
            cursor.execute("SELECT id FROM foo ORDER BY id")
            results = cursor.fetchall()
            assert len(results) == 2
            assert results[0]["id"] == 1
            assert results[1]["id"] == 2
    finally:
        mcontroller.stop()
        if s3controller:
            s3controller.stop()


def test_binlog_auto_rotation(master_controller):
    mcontroller, master = master_controller
    mcontroller.backup_settings["forced_binlog_rotation_interval"] = 1

    mcontroller.switch_to_active_mode()
    mcontroller.start()

    def streaming_binlogs():
        assert mcontroller.backup_streams
        assert mcontroller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog

    # Wait for the backup stream to reach stable state so there aren't unexpected binlog rotations
    while_asserts(streaming_binlogs, timeout=10)

    final_binlogs = set()
    initial_binlogs = set()
    with mysql_cursor(**master.connect_options) as cursor:
        cursor.execute("CREATE TABLE test_data (value TEXT)")
        cursor.execute("SHOW BINARY LOGS")
        for binlog in cursor.fetchall():
            initial_binlogs.add(binlog["Log_name"])

    start_time = time.monotonic()
    while time.monotonic() - start_time < 3.5:
        with mysql_cursor(**master.connect_options) as cursor:
            cursor.execute("INSERT INTO test_data (value) VALUES (%s)", [str(time.time())])
            cursor.execute("COMMIT")
            cursor.execute("SHOW BINARY LOGS")
            for binlog in cursor.fetchall():
                final_binlogs.add(binlog["Log_name"])

        time.sleep(0.1)

    new_binlogs = final_binlogs - initial_binlogs
    # We wait 3.5 seconds so depending on the timing there could be 3 or 4 binlogs
    assert len(new_binlogs) in {3, 4}

    # Ensure one more rotation is done (we previously wrote some data that was not included in latest rotation)
    mcontroller.rotate_and_back_up_binlog()
    with mysql_cursor(**master.connect_options) as cursor:
        cursor.execute("SHOW BINARY LOGS")
        final_binlogs = set(binlog["Log_name"] for binlog in cursor.fetchall())

    new_binlogs = final_binlogs - initial_binlogs
    assert len(new_binlogs) in {4, 5}

    # Binlog rotation will happen even if there are no changes
    time.sleep(1.5)
    with mysql_cursor(**master.connect_options) as cursor:
        cursor.execute("SHOW BINARY LOGS")
        final_binlogs = set(binlog["Log_name"] for binlog in cursor.fetchall())
        cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
        gtid_executed = cursor.fetchone()["gtid_executed"]

    new_binlogs = final_binlogs - initial_binlogs
    assert len(new_binlogs) > 4

    # Take new backup and ensure that has appropriate GTID executed value
    mcontroller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)

    # Wait for the upload to finish
    def has_multiple_backups():
        assert len(mcontroller.state["backups"]) == 2
        assert all(backup["completed_at"] for backup in mcontroller.state["backups"])
        assert len(mcontroller.backup_streams) == 1

    while_asserts(has_multiple_backups, timeout=15)
    bb_gtid_executed = mcontroller.backup_streams[-1].state["basebackup_info"]["gtid_executed"]
    assert bb_gtid_executed
    assert bb_gtid_executed == parse_gtid_range_string(gtid_executed)


def test_collect_binlogs_to_purge():
    now = time.time()
    binlogs = [
        {
            "local_index": 1,
            "gtid_ranges": [{"server_uuid": "uuid1", "start": 1, "end": 6}],
            "processed_at": now - 20,
        },
        {
            "local_index": 2,
            "gtid_ranges": [{"server_uuid": "uuid1", "start": 7, "end": 8}],
            "processed_at": now - 10,
        },
    ]
    purge_settings = {
        "min_binlog_age_before_purge": 30,
        "purge_when_observe_no_streams": True,
    }
    log = MagicMock()

    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=None,
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state={},
    )
    assert not binlogs_to_purge
    assert only_inapplicable_binlogs is True
    log.info.assert_called_with(
        "Binlog %s was processed %s seconds ago and min age before purging is %s seconds, not purging", 1, 21, 30
    )

    purge_settings["min_binlog_age_before_purge"] = 5
    bs1 = MagicMock()
    bs1.is_binlog_safe_to_delete.return_value = False
    bs2 = MagicMock()
    bs2.is_binlog_safe_to_delete.return_value = True
    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state={},
    )
    assert not binlogs_to_purge
    assert only_inapplicable_binlogs is False
    log.info.assert_called_with("Binlog %s reported not safe to delete by some backup streams", 1)

    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.observe,
        purge_settings=purge_settings,
        replication_state={},
    )
    assert not binlogs_to_purge
    assert only_inapplicable_binlogs is False
    log.info.assert_called_with(
        "Binlog %s either reported as unsafe to delete (%s) by some stream or not reported as safe to delete by any (%s)",
        1,
        True,
        True,
    )

    # No backup streams or replication state and observe node, should allow purging anything
    log = MagicMock()
    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=[],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.observe,
        purge_settings=purge_settings,
        replication_state={},
    )
    assert binlogs_to_purge == binlogs
    assert only_inapplicable_binlogs is False
    log.info.assert_any_call("No backup streams and purging is allowed, assuming purging %s is safe", 1)
    log.info.assert_any_call("No backup streams and purging is allowed, assuming purging %s is safe", 2)
    log.info.assert_any_call("No replication state set, assuming purging binlog %s is safe", 1)
    log.info.assert_any_call("No replication state set, assuming purging binlog %s is safe", 2)

    log = MagicMock()
    replication_state: Dict[str, GtidExecuted] = {
        "server1": {},
    }
    bs1.is_binlog_safe_to_delete.return_value = True
    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert not binlogs_to_purge
    assert only_inapplicable_binlogs is False
    log.info.assert_called_with("Binlog %s not yet replicated to server %r, not purging", 1, "server1")

    log = MagicMock()
    replication_state = {
        "server1": {"uuid1": [[1, 7]]},
    }
    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert binlogs_to_purge == binlogs[:1]
    assert only_inapplicable_binlogs is False
    log.info.assert_any_call("Binlog %s has been replicated to all servers, purging", 1)
    log.info.assert_any_call("Binlog %s not yet replicated to server %r, not purging", 2, "server1")

    log = MagicMock()
    replication_state = {
        "server1": {"uuid1": [[1, 8]]},
    }
    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert binlogs_to_purge == binlogs
    assert only_inapplicable_binlogs is False
    log.info.assert_any_call("Binlog %s has been replicated to all servers, purging", 1)
    log.info.assert_any_call("Binlog %s has been replicated to all servers, purging", 2)

    log = MagicMock()
    binlogs[0]["gtid_ranges"] = []
    binlogs[1]["gtid_ranges"] = []
    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert not binlogs_to_purge
    assert only_inapplicable_binlogs is True

    binlogs.append(
        {
            "local_index": 3,
            "gtid_ranges": [{"server_uuid": "uuid1", "start": 7, "end": 8}],
            "processed_at": now - 10,
        }
    )
    log = MagicMock()
    binlogs_to_purge, only_inapplicable_binlogs = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert binlogs_to_purge == binlogs
    assert only_inapplicable_binlogs is False
    log.info.assert_any_call("Binlog %s has been replicated to all servers, purging", 3)


def test_periodic_backup_based_on_exceeded_intervals(time_machine, master_controller) -> None:
    # pylint: disable=protected-access
    time_machine.move_to("2023-01-02T18:00:00")

    # By default backup_hour = 3, backup_interval_minutes = 1440
    m_controller, master = master_controller

    m_controller.switch_to_active_mode()
    m_controller.start()

    def streaming_binlogs(controller: Controller, expected_completed_backups: int):
        assert controller.backup_streams
        assert controller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog

        complete_backups = [backup for backup in controller.state["backups"] if backup["completed_at"]]
        assert len(complete_backups) == expected_completed_backups

    def flush_binlogs():
        with mysql_cursor(**master.connect_options) as cursor:
            cursor.execute("FLUSH BINARY LOGS")

    # write some data for the first backup
    flush_binlogs()
    # first backup for 2023-01-02 3:00 should be generated at 6pm (time we started the service)
    while_asserts(lambda: streaming_binlogs(m_controller, 1), timeout=10)

    # generate more data for second backup
    flush_binlogs()

    # second backup ideally should be done at 2023-01-03 03:00, but following the
    # "half of interval" rule (at least 12 hours of difference between most recent scheduled and current time)
    # therefore, it should be actually scheduled at 2023-01-03 06:00
    time_machine.move_to("2023-01-03T03:00:00+00:00")

    expected_normalized_time = datetime.datetime(2023, 1, 3, 3, tzinfo=datetime.timezone.utc)
    assert m_controller._current_normalized_backup_timestamp() == expected_normalized_time.isoformat()

    # no new backup should be scheduled
    time.sleep(1)
    min_created_at = datetime.datetime(2023, 1, 3, 3, tzinfo=datetime.timezone.utc).timestamp()
    assert not any(bs.created_at >= min_created_at for bs in m_controller.backup_streams)

    time_machine.move_to("2023-01-03T06:00:00+00:00")
    while_asserts(lambda: streaming_binlogs(m_controller, 2), timeout=10)

    # generate more data for third backup
    flush_binlogs()

    # After second backup, the next scheduled one should be at 2023-02-04 03:00:00, but let's change
    # backup_interval_minutes to 2880 (48 hours)
    m_controller.backup_settings["backup_interval_minutes"] = 2880

    time_machine.move_to("2023-01-04T06:00:00+00:00")

    expected_normalized_time = datetime.datetime(2023, 1, 3, 3, tzinfo=datetime.timezone.utc)
    assert m_controller._current_normalized_backup_timestamp() == expected_normalized_time.isoformat()

    time.sleep(1)
    # no new backup should be scheduled
    min_created_at = datetime.datetime(2023, 1, 4, 3, tzinfo=datetime.timezone.utc).timestamp()
    assert not any(bs.created_at >= min_created_at for bs in m_controller.backup_streams)

    time_machine.move_to("2023-01-05T03:00:00+00:00")
    expected_normalized_time = datetime.datetime(2023, 1, 5, 3, tzinfo=datetime.timezone.utc)

    assert m_controller._current_normalized_backup_timestamp() == expected_normalized_time.isoformat()
    while_asserts(lambda: streaming_binlogs(m_controller, 3), timeout=10)


def test_changed_backup_hour_is_applied(time_machine, master_controller) -> None:
    # pylint: disable=protected-access
    time_machine.move_to("2023-01-02T03:30:00")

    # By default backup_hour = 3, backup_interval_minutes = 1440
    m_controller, master = master_controller

    m_controller.switch_to_active_mode()
    m_controller.start()

    def streaming_binlogs(controller: Controller, expected_completed_backups: int):
        assert controller.backup_streams
        assert controller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog

        complete_backups = [backup for backup in controller.state["backups"] if backup["completed_at"]]
        assert len(complete_backups) == expected_completed_backups

    def flush_binlogs():
        with mysql_cursor(**master.connect_options) as cursor:
            cursor.execute("FLUSH BINARY LOGS")

    # write some data for the first backup
    flush_binlogs()
    # first backup for 2023-01-02 03:00 should be generated at 03:30 (time we started the service)
    while_asserts(lambda: streaming_binlogs(m_controller, 1), timeout=10)

    # generate more data for second backup
    flush_binlogs()

    time_machine.move_to("2023-01-03T03:00:00+00:00")

    expected_normalized_time = datetime.datetime(2023, 1, 3, 3, tzinfo=datetime.timezone.utc)
    assert m_controller._current_normalized_backup_timestamp() == expected_normalized_time.isoformat()

    time.sleep(1)
    min_created_at = datetime.datetime(2023, 1, 3, 3, tzinfo=datetime.timezone.utc).timestamp()
    assert any(bs.created_at >= min_created_at for bs in m_controller.backup_streams)

    time_machine.move_to("2023-01-03T06:00:00+00:00")
    while_asserts(lambda: streaming_binlogs(m_controller, 2), timeout=10)

    # generate more data for third backup
    flush_binlogs()

    # After second backup, the next scheduled one should be at 2023-02-04 03:00:00, but let's change
    # the backup hour to 18
    m_controller.backup_settings["backup_hour"] = 18

    time_machine.move_to("2023-01-04T18:00:00+00:00")

    expected_normalized_time = datetime.datetime(2023, 1, 4, 18, tzinfo=datetime.timezone.utc)
    assert m_controller._current_normalized_backup_timestamp() == expected_normalized_time.isoformat()

    time.sleep(1)
    min_created_at = datetime.datetime(2023, 1, 4, 3, tzinfo=datetime.timezone.utc).timestamp()
    assert any(bs.created_at >= min_created_at for bs in m_controller.backup_streams)

    time_machine.move_to("2023-01-05T18:00:00+00:00")
    expected_normalized_time = datetime.datetime(2023, 1, 5, 18, tzinfo=datetime.timezone.utc)

    assert m_controller._current_normalized_backup_timestamp() == expected_normalized_time.isoformat()
    while_asserts(lambda: streaming_binlogs(m_controller, 3), timeout=10)


def test_mark_backup_preservation(
    time_machine,
    master_controller,
) -> None:
    m_controller, _ = master_controller

    # just hold 3 backups at most
    m_controller.backup_settings["backup_count_max"] = 2
    m_controller.backup_settings["backup_count_min"] = 1

    # drop every backup older than 2 days
    m_controller.backup_settings["backup_age_days_max"] = 2

    m_controller.switch_to_active_mode()

    def wait_for_scheduled_backup(backup_num):
        completed_backups = [backup for backup in m_controller.state["backups"] if backup["completed_at"] is not None]
        assert len(completed_backups) == backup_num

    def backup_has_preservation(stream_id):
        for backup in m_controller.state["backups"]:
            if backup["stream_id"] == stream_id:
                assert backup["preserve_until"] is not None
                break
        else:
            assert False, f"Backup with stream id {stream_id} not found."

    current_date = datetime.datetime(2023, 9, 6)
    time_machine.move_to(current_date)
    m_controller.start()

    # wait for first backup to be created
    while_asserts(lambda: wait_for_scheduled_backup(1), timeout=20)
    first_stream_id = m_controller.state["backups"][0]["stream_id"]

    # wait for second backup and preserve it
    current_date += datetime.timedelta(days=1)
    time_machine.move_to(current_date)
    while_asserts(lambda: wait_for_scheduled_backup(2), timeout=20)

    stream_id_to_preserve = [
        backup["stream_id"] for backup in m_controller.state["backups"] if backup["stream_id"] != first_stream_id
    ][0]
    preserved_until = datetime.datetime(2023, 9, 15, tzinfo=datetime.timezone.utc)
    m_controller.mark_backup_preservation(
        stream_id=stream_id_to_preserve,
        preserve_until=preserved_until,
    )
    while_asserts(lambda: backup_has_preservation(stream_id_to_preserve))

    # wait for third backup and wait for first backup to be removed
    current_date += datetime.timedelta(days=1)
    time_machine.move_to(current_date)
    while_asserts(lambda: wait_for_scheduled_backup(2), timeout=20)

    # check that the first backup was deleted (due to max backup count was reached and backup is old enough)
    wait_for_condition(
        lambda: not any(backup for backup in m_controller.state["backups"] if backup["stream_id"] == first_stream_id)
    )

    # generate 2 new backups, we should exceed maximum backup count
    current_date += datetime.timedelta(days=1)
    time_machine.move_to(current_date)
    while_asserts(lambda: wait_for_scheduled_backup(3), timeout=20)

    current_date += datetime.timedelta(days=1)
    time_machine.move_to(current_date)
    while_asserts(lambda: wait_for_scheduled_backup(4), timeout=20)

    # now exceed preservation date and extra backups should be purged as well (max age exceeded)
    time_machine.move_to(preserved_until + datetime.timedelta(minutes=10))
    while_asserts(lambda: wait_for_scheduled_backup(2), timeout=20)


def test_unmark_backup_preservation(
    time_machine,
    master_controller,
) -> None:
    m_controller, _ = master_controller

    # just hold 1 backup at most
    m_controller.backup_settings["backup_count_max"] = 1
    m_controller.backup_settings["backup_count_min"] = 1
    m_controller.backup_settings["backup_age_days_max"] = 1

    m_controller.switch_to_active_mode()
    time_machine.move_to(datetime.datetime(2023, 9, 6))
    m_controller.start()

    wait_for_condition(
        lambda: len([backup for backup in m_controller.state["backups"] if backup["completed_at"] is not None]), timeout=20
    )

    # preserve the backup
    preserve_until = datetime.datetime(2023, 9, 10)
    stream_id_to_preserve = m_controller.state["backups"][0]["stream_id"]
    m_controller.mark_backup_preservation(stream_id=stream_id_to_preserve, preserve_until=preserve_until)
    wait_for_condition(lambda: m_controller.state["backups"][0]["preserve_until"] is not None, timeout=10)

    time_machine.move_to(datetime.datetime(2023, 9, 7))
    wait_for_condition(lambda: len(m_controller.state["backups"]) == 2, timeout=20)

    time_machine.move_to(datetime.datetime(2023, 9, 8))
    wait_for_condition(lambda: len(m_controller.state["backups"]) == 3, timeout=20)

    time_machine.move_to(datetime.datetime(2023, 9, 9))
    m_controller.mark_backup_preservation(stream_id=stream_id_to_preserve, preserve_until=None)
    wait_for_condition(lambda: len(m_controller.state["backups"]) == 1, timeout=30)


@patch.object(RestoreCoordinator, "MAX_BASEBACKUP_ERRORS", 2)
@patch.object(BasebackupRestoreOperation, "restore_backup", side_effect=Exception("failed restoring basebackup"))
def test_backup_marked_as_broken_after_failed_restoration(
    mocked_restore_backup: MagicMock,  # pylint: disable=unused-argument
    default_backup_site,
    master_controller,
    mysql_empty,
    session_tmpdir,
) -> None:
    m_controller, master = master_controller
    new_controller = None
    try:
        m_controller.switch_to_active_mode()
        m_controller.start()

        # Write some data that gets included in the first backup
        with mysql_cursor(**master.connect_options) as cursor:
            cursor.execute("CREATE TABLE foo (id INTEGER)")
            cursor.execute("INSERT INTO foo VALUES (1)")
            cursor.execute("COMMIT")
            cursor.execute("FLUSH BINARY LOGS")

        def streaming_binlogs():
            assert m_controller.backup_streams
            assert len(m_controller.backup_streams) == 1
            assert m_controller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog

        while_asserts(streaming_binlogs, timeout=15)

        new_controller = build_controller(
            Controller,
            default_backup_site=default_backup_site,
            mysql_config=mysql_empty,
            session_tmpdir=session_tmpdir,
        )
        new_controller.start()

        # fetch remote backup
        wait_for_condition(
            lambda: new_controller.state["backups_fetched_at"] != 0 and len(new_controller.state["backups"]) == 1, timeout=2
        )

        assert new_controller.state["backups"][0]["broken_at"] is None

        # try to restore
        new_controller.restore_backup(
            site=new_controller.state["backups"][0]["site"], stream_id=new_controller.state["backups"][0]["stream_id"]
        )

        def restoration_is_failed():
            assert new_controller.restore_coordinator
            assert new_controller.restore_coordinator.phase == RestoreCoordinator.Phase.failed

        while_asserts(restoration_is_failed, timeout=40)

        refreshed_backup_lists = new_controller.get_backup_list(backup_sites=new_controller.backup_sites)
        assert refreshed_backup_lists[0]["broken_at"] is not None

    finally:
        m_controller.stop()
        if new_controller:
            new_controller.stop()


@patch.object(RestoreCoordinator, "MAX_BASEBACKUP_ERRORS", 2)
def test_restore_failed_basebackup_and_retry_with_prior(
    default_backup_site,
    master_controller,
    mysql_empty,
    session_tmpdir,
) -> None:
    m_controller, master = master_controller
    new_controller = None

    mysql_empty.connect_options["password"] = master.connect_options["password"]

    try:
        data_generator = DataGenerator(
            connect_info=master.connect_options,
            make_temp_tables=False,
        )
        data_generator.start()

        m_controller.switch_to_active_mode()
        m_controller.start()

        def streaming_binlogs():
            assert m_controller.backup_streams
            assert all(bs.active_phase == BackupStream.ActivePhase.binlog for bs in m_controller.backup_streams)

        while_asserts(streaming_binlogs, timeout=10)

        m_controller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)
        data_generator.stop()

        def all_backups_completed(expected_num):
            backups = m_controller.state["backups"]
            assert len(backups) == expected_num
            assert all(backup["completed_at"] is not None for backup in backups)

        while_asserts(lambda: all_backups_completed(expected_num=2), timeout=30)

        m_controller.stop()

        # get the most recent backup (corrupted)
        corrupted_backup = sort_completed_backups(m_controller.state["backups"])[1]

        new_controller = build_controller(
            Controller,
            default_backup_site=default_backup_site,
            mysql_config=mysql_empty,
            session_tmpdir=session_tmpdir,
        )

        new_controller.start()

        restore_operation_side_effects = LimitedSideEffectMock(
            original_function_to_mock=BasebackupRestoreOperation.restore_backup,
            max_calls_with_side_effects=2,
        )

        # raise exceptions for all attempts to restoring corrupted basebackup,
        # when trying to restore the previous basebackup, it should work as we expect
        with patch.object(
            BasebackupRestoreOperation,
            "restore_backup",
            autospec=True,
            side_effect=restore_operation_side_effects,
        ) as mocked_restore_backup:
            wait_for_condition(lambda: new_controller.state["backups_fetched_at"] != 0, timeout=2)

            new_controller.restore_backup(site=corrupted_backup["site"], stream_id=corrupted_backup["stream_id"])

            def mocked_raised_all_errors() -> None:
                assert mocked_restore_backup.call_count > 2

            def restoration_has_phase(phase):
                assert new_controller.restore_coordinator is not None
                assert new_controller.restore_coordinator.state["phase"] == phase

            # second basebackup should fail due to the side effects
            while_asserts(mocked_raised_all_errors, timeout=30)

            # after second basebackup fails, it should try to restore the first backup we generated
            # this one should be successfull
            while_asserts(lambda: restoration_has_phase(RestoreCoordinator.Phase.completed), timeout=40)

            new_controller.get_backup_list(backup_sites=new_controller.backup_sites)

            current_backups = sort_completed_backups(new_controller.state["backups"])
            corrupted_backup = current_backups[1]
            # failed basebackup should be marked as broken
            assert corrupted_backup["broken_at"] is not None

            # verify the empty mysql restored all data.
            with mysql_cursor(**mysql_empty.connect_options) as cursor:
                expected_row_count = data_generator.row_count
                cursor.execute("SELECT COUNT(*) AS count FROM db1.t1")
                result_row_count = cast(CountDict, cursor.fetchone())["count"]
                assert expected_row_count == result_row_count
    finally:
        m_controller.stop()
        if new_controller:
            new_controller.stop()


@patch.object(BackupStream, "remove", autospec=True)
def test_purge_old_backups_should_not_remove_read_only_backups(
    mocked_backup_stream_remove: MagicMock,
    default_backup_site,
    mysql_empty,
    session_tmpdir,
) -> None:
    # pylint: disable=protected-access
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    controller.backup_settings["backup_count_min"] = 0
    controller.backup_settings["backup_count_max"] = 1
    # set it to 1 day
    controller.backup_settings["backup_age_days_max"] = 1

    def remove_backup(backup_stream) -> None:
        controller.state["backups"] = [
            backup for backup in controller.state["backups"] if backup["stream_id"] != backup_stream.stream_id
        ]

    mocked_backup_stream_remove.side_effect = remove_backup

    ten_years_old = time.time() - 3650 * 24 * 60 * 60

    def _append_backup(stream_id: str, ts: float, read_only: bool = True) -> None:
        controller.state["backups"].append(
            {
                "basebackup_info": {
                    "checkpoints_file_content": None,
                    "end_ts": ts,
                    "incremental": False,
                    "tool_version": "8.0.30",
                },
                "closed_at": ts,
                "completed_at": ts,
                "broken_at": None,
                "preserve_until": None,
                "recovery_site": read_only,
                "stream_id": stream_id,
                "resumable": True,
                "site": "default",
            }
        )

    _append_backup(stream_id="1", ts=ten_years_old, read_only=True)  # no matter how old, should be kept if read-only
    _append_backup(stream_id="2", ts=ten_years_old, read_only=False)  # old backup, should be purged
    _append_backup(stream_id="3", ts=time.time(), read_only=False)  # latest backup

    controller._purge_old_backups()
    backup_list = controller.state["backups"]
    assert len(backup_list) == 2
    assert all(b["stream_id"] != "2" for b in backup_list)  # backup "2" should be deleted


def add_fake_backup(
    controller: Controller,
    stream_id: str,
    incremental: bool = False,
    completed_at: float | None = time.time() - 5 * 60,
    closed_at: float | None = time.time() - 3 * 60,
    broken_at: float | None = None,
    preserve_until: str | None = None,
) -> Backup:
    now = time.time()
    backup = Backup(
        basebackup_info=BaseBackup(
            checkpoints_file_content=None,
            end_ts=now - 20 * 60,
            incremental=incremental,
            tool_version="8.0.30",
        ),
        closed_at=closed_at,
        completed_at=completed_at,
        broken_at=broken_at,
        preserve_until=preserve_until,
        recovery_site=False,
        stream_id=stream_id,
        resumable=True,
        site="default",
    )
    backups = controller.state["backups"]
    backups.append(backup)
    controller.state_manager.update_state(backups=backups)
    return backup


def patched_remove_backup(controller: Controller) -> Callable:
    def remove_backup(backup_stream: BackupStream) -> None:
        controller.state["backups"] = [
            backup for backup in controller.state["backups"] if backup["stream_id"] != backup_stream.stream_id
        ]

    return remove_backup


@patch.object(BackupStream, "remove", autospec=True)
def test_purge_old_backups_exceeding_backup_age_days_max(
    mocked_backup_stream_remove: MagicMock,
    default_backup_site,
    mysql_empty,
    session_tmpdir,
) -> None:
    # pylint: disable=protected-access
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    controller.backup_settings["backup_count_min"] = 2
    controller.backup_settings["backup_count_max"] = 3
    # set it to 1 sec
    controller.backup_settings["backup_age_days_max"] = 1 / 86400

    # 1 / 86400 * 24 * 60 * 60

    mocked_backup_stream_remove.side_effect = patched_remove_backup(controller)

    time.sleep(1)
    add_fake_backup(controller, "1")
    add_fake_backup(controller, "2")

    # no backups should be purged, since we didn't exceed the max
    controller._purge_old_backups()
    assert len(controller.state["backups"]) == 2

    # exceed the backup limit
    add_fake_backup(controller, "3")
    add_fake_backup(controller, "4")
    controller._purge_old_backups()

    # two first backups should have been removed
    assert len(controller.state["backups"]) == 2
    assert controller.state["backups"][0]["stream_id"] == "3"
    assert controller.state["backups"][1]["stream_id"] == "4"

    add_fake_backup(controller, "5")
    add_fake_backup(controller, "6")

    # mark all backups as broken, except the oldest one
    # in this case we should not remove any backup even if we exceed the max
    for bid in range(1, len(controller.state["backups"])):
        controller.state["backups"][bid]["broken_at"] = time.time()

    controller._purge_old_backups()
    assert len(controller.state["backups"]) == 4

    # now add some healthy backups and exceed limit, it should purge backup 3
    add_fake_backup(controller, "7")
    add_fake_backup(controller, "8")
    add_fake_backup(controller, "9")

    controller._purge_old_backups()
    assert len(controller.state["backups"]) == 6
    assert controller.state["backups"][0]["stream_id"] == "4"


@pytest.mark.parametrize(
    "backups_data,expected_result",
    [
        pytest.param([], 0, id="No backups"),
        pytest.param([(False, None), (False, None), (False, None)], 3, id="All full intact backups"),
        pytest.param([(False, None), (False, time.time()), (False, None)], 2, id="All full backups, one broken"),
        pytest.param([(False, None), (True, None), (True, None)], 3, id="Full backup and two incremental, all intact"),
        pytest.param([(False, time.time()), (True, None), (True, None)], 0, id="Incremental backups, full broken"),
        pytest.param(
            [(False, None), (True, time.time()), (True, None)], 1, id="Incremental backups, incremental in the middle broken"
        ),
        pytest.param([(False, None), (True, None), (True, time.time())], 2, id="Incremental backups, last one broken"),
        pytest.param(
            [(False, time.time()), (True, None), (True, None), (False, None)],
            1,
            id="Incremental backups, multiple full backups, first one broken",
        ),
        pytest.param(
            [(False, None), (True, None), (True, time.time()), (False, None)],
            3,
            id="Incremental backups, multiple full backups, one incremental broken",
        ),
        pytest.param(
            [(False, None), (True, None), (True, None), (False, None), (True, None)], 5, id="Incremental backups, all intact"
        ),
    ],
)
def test_count_non_broken_backups(
    backups_data: list[tuple[bool, float | None]], expected_result: int, default_backup_site, mysql_empty, session_tmpdir
):
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    for idx, b in enumerate(backups_data):
        incremental, broken_at = b
        add_fake_backup(controller, str(idx), incremental=incremental, broken_at=broken_at)

    assert Controller.count_non_broken_backups(controller.state["backups"]) == expected_result


@patch.object(BackupStream, "remove", autospec=True)
def test_purge_old_incremental_backups_exceeding_backup_age_days_max(
    mocked_backup_stream_remove: MagicMock,
    default_backup_site,
    mysql_empty,
    session_tmpdir,
) -> None:
    # pylint: disable=protected-access
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    controller.backup_settings["backup_count_min"] = 2
    controller.backup_settings["backup_count_max"] = 3
    # set it to 1 sec
    controller.backup_settings["backup_age_days_max"] = 1 / 86400

    mocked_backup_stream_remove.side_effect = patched_remove_backup(controller)

    time.sleep(1)
    add_fake_backup(controller, "1")
    add_fake_backup(controller, "2", incremental=True)
    add_fake_backup(controller, "3", incremental=True)
    add_fake_backup(controller, "4", incremental=True)
    controller._purge_old_backups()

    # backup 1 would have been removed if it did not have following incremental backups
    assert len(controller.state["backups"]) == 4

    # Add full backup and two incremental ones to that previous backups could be purged
    add_fake_backup(controller, "5")
    add_fake_backup(controller, "6", incremental=True)
    add_fake_backup(controller, "7", incremental=True)
    controller._purge_old_backups()
    assert len(controller.state["backups"]) == 3

    assert controller.state["backups"][0]["stream_id"] == "5"
    assert controller.state["backups"][1]["stream_id"] == "6"
    assert controller.state["backups"][2]["stream_id"] == "7"


@pytest.mark.parametrize(
    "backup_has_data,stream_statuses_to_be_marked,result_status_keys_must_have_value",
    [
        (False, ["completed", "closed"], {"completed_at": False, "closed_at": False, "broken_at": False}),
        (True, ["completed"], {"completed_at": True, "closed_at": False, "broken_at": False}),
        (True, ["closed"], {"completed_at": False, "closed_at": False, "broken_at": False}),
        (True, ["completed", "closed"], {"completed_at": True, "closed_at": True, "broken_at": False}),
        (True, ["completed", "closed", "broken"], {"completed_at": True, "closed_at": True, "broken_at": True}),
    ],
)
def test_check_if_get_backup_list_fills_attributes_based_on_missing_files(
    backup_has_data: bool,
    stream_statuses_to_be_marked: List[str],
    result_status_keys_must_have_value: Dict[str, bool],
    default_backup_site,
    mysql_empty,
    session_tmpdir,
) -> None:
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )

    site_config = controller.backup_sites["default"]
    file_storage = get_transfer(site_config["object_storage"])

    # store fake data for stream 1
    file_storage.store_file_from_memory("default/1/basebackup.xbstream", b"\x0001\x0002" if backup_has_data else b"")
    file_storage.store_file_from_memory("default/1/basebackup.json", b'{"binlog_name": "000001.bin"}')

    for status in stream_statuses_to_be_marked:
        file_storage.store_file_from_memory(
            f"default/1/{status}.json",
            b"",
            make_fs_metadata({f"{status}_at": time.time()}),
        )

    result = controller.get_backup_list(backup_sites=controller.backup_sites)[0]

    # it is resumable if backup has data and basebackup.json has information about it
    assert result["resumable"] is backup_has_data

    for key, has_value in result_status_keys_must_have_value.items():
        assert key in result
        if has_value:
            assert result[key] is not None
        else:
            assert result[key] is None


class LimitedSideEffectMock:
    """
    Instantiate class when trying to patch an object with side effects
    only for a limited amount of times and later go back to its
    original behavior.
    """

    calls = 0

    def __init__(
        self,
        original_function_to_mock,
        max_calls_with_side_effects: int,
    ) -> None:
        self.original_function_to_mock = original_function_to_mock
        self.max_calls_with_side_effects = max_calls_with_side_effects

    def __call__(self, *args, **kwargs) -> Any:
        if self.calls < self.max_calls_with_side_effects:
            self.calls += 1
            raise Exception()

        return self.original_function_to_mock(*args, **kwargs)


def test_get_incremental_backup_info(default_backup_site, mysql_empty, session_tmpdir):

    backup1 = Backup(
        basebackup_info=BaseBackup(
            incremental=False,
            end_ts=1.0,
            checkpoints_file_content="backup_type = full-backuped...",
            tool_version=xtrabackup_version_to_string(get_xtrabackup_version()),
        ),
        closed_at=1,
        completed_at=1,
        recovery_site=False,
        stream_id="1000",
        resumable=False,
        site="not_default_site",
        broken_at=None,
        preserve_until=None,
    )
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    import copy

    backup2 = copy.deepcopy(backup1)
    backup2["basebackup_info"]["incremental"] = True
    backup2["basebackup_info"]["checkpoints_file_content"] = "backup_type = incremental..."
    backup2["completed_at"] = 2
    backup2["closed_at"] = 2
    backup2["stream_id"] = "1001"

    backup3 = copy.deepcopy(backup1)
    backup3["basebackup_info"]["incremental"] = True
    backup3["basebackup_info"]["checkpoints_file_content"] = "backup_type = incremental..."
    backup3["completed_at"] = 3
    backup3["closed_at"] = 3
    backup3["stream_id"] = "1002"

    controller.state["backups"] = [backup1, backup2, backup3]
    info = controller.get_incremental_backup_info()
    assert info["required_streams"] == ["1000", "1001", "1002"]
    assert info["last_checkpoint"] == "backup_type = incremental..."

    # Breaking one of the backups or having no checkpoint should return nothing, as incremental backup is not possible
    for b in [backup1, backup2, backup3]:
        b["broken_at"] = 1
        assert controller.get_incremental_backup_info() is None
        b["broken_at"] = None

    for b in [backup1, backup2, backup3]:
        checkpoint = b["basebackup_info"]["checkpoints_file_content"]
        b["basebackup_info"]["checkpoints_file_content"] = None
        assert controller.get_incremental_backup_info() is None
        b["basebackup_info"]["checkpoints_file_content"] = checkpoint

    assert controller.get_incremental_backup_info() is not None

    # For whatever reason there is no full backup in the list
    backup1["basebackup_info"]["incremental"] = True
    assert controller.get_incremental_backup_info() is None

    # We have only one full backup
    backup1["basebackup_info"]["incremental"] = False
    controller.state["backups"] = [backup1]
    info = controller.get_incremental_backup_info()
    assert info["required_streams"] == ["1000"]
    assert info["last_checkpoint"] == "backup_type = full-backuped..."


@pytest.mark.parametrize(
    "backup_tool_version,tool_version,expected_incremental",
    ((None, "8.0.30-12", False), ("8.0.30-10", "8.0.30-12", False), ("8.0.30-12", "8.0.30-12", True)),
)
def test_get_incremental_backup_info_version_check(
    default_backup_site,
    mysql_empty,
    session_tmpdir,
    backup_tool_version: str | None,
    tool_version: str,
    expected_incremental: bool,
) -> None:
    backup1 = Backup(
        basebackup_info=BaseBackup(
            incremental=False,
            end_ts=1.0,
            checkpoints_file_content="backup_type = full-backuped...",
            tool_version=backup_tool_version,
        ),
        closed_at=1,
        completed_at=1,
        recovery_site=False,
        stream_id="1000",
        resumable=False,
        site="not_default_site",
        broken_at=None,
        preserve_until=None,
    )
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    controller.state["backups"] = [backup1]
    with patch("myhoard.controller.get_xtrabackup_version", return_value=parse_version(tool_version)):
        info = controller.get_incremental_backup_info()
        if expected_incremental:
            assert info
            assert info["required_streams"] == ["1000"]
            assert info["last_checkpoint"] == "backup_type = full-backuped..."
        else:
            assert info is None


def test_should_schedule_incremental_backup(
    time_machine,
    master_controller,
) -> None:
    m_controller, _ = master_controller

    m_controller.backup_settings["incremental"] = {"enabled": False}
    # pylint: disable=protected-access
    assert not m_controller._should_schedule_incremental_backup()

    m_controller.backup_settings["incremental"] = {"enabled": True, "full_backup_week_schedule": "wed"}
    # Travel to Wed
    time_machine.move_to(datetime.datetime(2025, 2, 5))
    # pylint: disable=protected-access
    assert not m_controller._should_schedule_incremental_backup()

    time_machine.move_to(datetime.datetime(2025, 2, 6))
    # pylint: disable=protected-access
    assert m_controller._should_schedule_incremental_backup()


@pytest.mark.parametrize(
    "backups,expected_no_purge_reason,travel_to_datetime,backup_settings_extra",
    [
        pytest.param([], Controller.NoPurgeReason.no_completed_backups, None, None, id="No backups"),
        pytest.param(
            [{"stream_id": "stream1", "completed_at": None}, {"stream_id": "stream2", "completed_at": None}],
            Controller.NoPurgeReason.no_completed_backups,
            None,
            None,
            id="No complete backups",
        ),
        pytest.param(
            [{"stream_id": "stream1", "incremental": True}], None, None, None, id="Drop leading incremental backup"
        ),
        pytest.param(
            [{"stream_id": "stream1", "incremental": True}, {"stream_id": "stream2", "incremental": True}],
            None,
            None,
            None,
            id="Drop leading incremental backup",
        ),
        pytest.param(
            [
                {"stream_id": "stream1", "incremental": True},
                {"stream_id": "stream2"},
                {"stream_id": "stream3"},
            ],
            None,
            None,
            None,
            id="Drop leading incremental backup",
        ),
        pytest.param(
            [
                {"stream_id": "stream1"},
                {"stream_id": "stream2", "incremental": True},
                {"stream_id": "stream3", "incremental": True},
                {"stream_id": "stream4", "incremental": True, "broken_at": 1},
            ],
            Controller.NoPurgeReason.within_max_amount,
            None,
            None,
            id="More than max backups, but not with broken ones",
        ),
        pytest.param(
            [
                {"stream_id": "stream1"},
                {"stream_id": "stream2", "incremental": True},
                {"stream_id": "stream3", "incremental": True, "broken_at": 1},
            ],
            Controller.NoPurgeReason.min_amount_required,
            None,
            None,
            id="Less than required min backups amount excluding broken",
        ),
        pytest.param(
            [
                {"stream_id": "stream1"},
                {"stream_id": "stream2", "incremental": True, "closed_at": None},
                {
                    "stream_id": "stream3",
                },
                {"stream_id": "stream4", "incremental": True},
            ],
            Controller.NoPurgeReason.non_closed_backup,
            None,
            None,
            id="Non closed incremental backup should prevent purging full backup",
        ),
        pytest.param(
            [
                {"stream_id": "stream1", "closed_at": None},
                {"stream_id": "stream2", "incremental": True},
                {"stream_id": "stream3"},
                {"stream_id": "stream4", "incremental": True},
            ],
            Controller.NoPurgeReason.non_closed_backup,
            None,
            None,
            id="Non closed backup should prevent purging",
        ),
        pytest.param(
            [
                {
                    "stream_id": "stream1",
                    "preserve_until": (
                        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
                    ).isoformat(),
                },
                {"stream_id": "stream2"},
                {"stream_id": "stream3"},
                {"stream_id": "stream4"},
            ],
            Controller.NoPurgeReason.preserved_backup,
            None,
            None,
            id="Full backup is preserved",
        ),
        pytest.param(
            [
                {"stream_id": "stream1"},
                {
                    "stream_id": "stream2",
                    "preserve_until": (
                        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
                    ).isoformat(),
                },
                {"stream_id": "stream3"},
                {"stream_id": "stream4"},
            ],
            None,
            None,
            None,
            id="One of full backups is preserved",
        ),
        pytest.param(
            [
                {"stream_id": "stream1"},
                {
                    "stream_id": "stream2",
                    "preserve_until": (
                        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
                    ).isoformat(),
                    "incremental": True,
                },
                {"stream_id": "stream3"},
                {"stream_id": "stream4"},
            ],
            Controller.NoPurgeReason.preserved_backup,
            None,
            None,
            id="Incremental backup is preserved",
        ),
        pytest.param(
            [
                {"stream_id": "stream1", "closed_at": time.time() - 24 * 60 * 60},
                {
                    "stream_id": "stream2",
                    "closed_at": time.time(),
                    "incremental": True,
                },
                {"stream_id": "stream3", "incremental": True},
                {"stream_id": "stream4"},
                {"stream_id": "stream5", "incremental": True},
            ],
            Controller.NoPurgeReason.required_backup_within_retention,
            None,
            {"backup_age_days_max": 1},
            id="Full backup expired, but incremental one did not",
        ),
        pytest.param(
            [
                {"stream_id": "stream1", "closed_at": time.time() - 24 * 60 * 60},
                {
                    "stream_id": "stream2",
                    "closed_at": time.time() - 24 * 60 * 60,
                    "incremental": True,
                },
                {"stream_id": "stream3"},
                {"stream_id": "stream4"},
                {"stream_id": "stream5"},
                {"stream_id": "stream6", "incremental": True},
            ],
            None,
            None,
            {"backup_age_days_max": 1},
            id="Backups out of retention",
        ),
        pytest.param(
            [
                {"stream_id": "stream1", "closed_at": time.time()},
                {"stream_id": "stream2"},
                {"stream_id": "stream3"},
            ],
            Controller.NoPurgeReason.requirements_not_met,
            None,
            {"backup_age_days_max": 1},
            id="Nothing to purge, no old backups, max threshold not reached",
        ),
        pytest.param(
            [
                {"stream_id": "stream1", "closed_at": time.time() - 24 * 60 * 60},
                {"stream_id": "stream2"},
                {"stream_id": "stream3", "incremental": True},
            ],
            Controller.NoPurgeReason.min_amount_required,
            None,
            {"backup_age_days_max": 1, "backup_count_min": 3, "backup_count_max": 5},
            id="Purging old backup would leave less than min backups",
        ),
    ],
)
def test_get_backup_to_purge(
    time_machine,
    backups: list[Dict[str, Any]],
    default_backup_site,
    mysql_empty,
    session_tmpdir,
    expected_no_purge_reason: Controller.NoPurgeReason | None,
    travel_to_datetime: datetime.datetime | None,
    backup_settings_extra: Dict[str, Any] | None,
):
    # pylint: disable=protected-access
    controller = build_controller(
        Controller,
        default_backup_site=default_backup_site,
        mysql_config=mysql_empty,
        session_tmpdir=session_tmpdir,
    )
    controller.backup_settings["backup_count_min"] = 2
    controller.backup_settings["backup_count_max"] = 3
    controller.backup_settings["backup_age_days_max"] = 1 / 86400
    if backup_settings_extra:
        controller.backup_settings.update(backup_settings_extra)
    if travel_to_datetime is not None:
        time_machine.move_to(travel_to_datetime)

    for b in backups:
        add_fake_backup(controller, **b)
    if expected_no_purge_reason is None:
        assert controller._get_backup_to_purge() == controller.state["backups"][0]
    else:
        assert controller._get_backup_to_purge() == expected_no_purge_reason
