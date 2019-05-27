# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import time
from unittest.mock import MagicMock

import pytest
from myhoard.backup_stream import BackupStream
from myhoard.controller import Controller
from myhoard.util import (change_master_to, mysql_cursor, parse_gtid_range_string, partition_sort_and_combine_gtid_ranges)

from . import (DataGenerator, build_controller, wait_for_condition, while_asserts)

pytestmark = [pytest.mark.unittest, pytest.mark.all]


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
    s3controller = None

    # Empty server will be initialized from backup that has been created from master so it'll use the same password
    # (normally all servers should be restored from same backup but we're not simulating that here now)
    mysql_empty["connect_options"]["password"] = master["connect_options"]["password"]

    with mysql_cursor(**master["connect_options"]) as cursor:
        cursor.execute("SELECT @@GLOBAL.server_uuid AS server_uuid")
        original_server_uuid = cursor.fetchone()["server_uuid"]

    # Set unknown replication state to all controllers so that they won't purge binary logs
    for controller in [mcontroller, s1controller, s2controller]:
        controller.state_manager.update_state(replication_state={"s1": {}})
        controller.binlog_purge_settings["min_binlog_age_before_purge"] = 1
        controller.binlog_purge_settings["purge_interval"] = 0.1

    master_dg = DataGenerator(connect_info=master["connect_options"], make_temp_tables=False)
    new_master_dg = None
    try:
        master_dg.start()

        phase_duration = 0.5
        time.sleep(phase_duration)
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
                with mysql_cursor(**standby["connect_options"]) as cursor:
                    cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
                    gtid_executed = cursor.fetchone()["gtid_executed"]
                    if gtid_executed != last_gtid_executeds[idx]:
                        cursor.execute("FLUSH BINARY LOGS")
                        last_gtid_executeds[idx] = gtid_executed
            if backup_count > 1:
                # Do some flushes on master to ensure there are simultaneous binlog uploads and existing ones get reused
                with mysql_cursor(**master["connect_options"]) as cursor:
                    cursor.execute("CREATE TABLE foo_{} (id INTEGER)".format(str(time.time()).replace(".", "_")))
                    cursor.execute("COMMIT")
                    cursor.execute("FLUSH BINARY LOGS")
            assert s1controller.backup_streams
            assert s2controller.backup_streams
            assert mcontroller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog
            complete_backups = [backup for backup in mcontroller.state["backups"] if backup["completed_at"]]
            assert len(complete_backups) == backup_count

        while_asserts(lambda: streams_available_master_streaming_binlogs(1), timeout=15)

        # Take another backup so that we get to test that existing uploads are reused (remote copied)
        mcontroller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)
        while_asserts(lambda: streams_available_master_streaming_binlogs(2), timeout=15)
        mcontroller.stats.increase.assert_any_call("myhoard.binlog.remote_copy")
        mcontroller.stats.increase.assert_any_call("myhoard.binlog.upload")

        for _ in range(5):
            time.sleep(phase_duration / 5)
            for standby in [standby1, standby2]:
                with mysql_cursor(**standby["connect_options"]) as cursor:
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
        with mysql_cursor(**standby1["connect_options"]) as cursor1:
            with mysql_cursor(**standby2["connect_options"]) as cursor2:
                cursor1.execute("STOP SLAVE IO_THREAD")
                cursor2.execute("STOP SLAVE IO_THREAD")

                # Wait for SQL threads to apply any relay logs that got downloaded from master
                def relay_log_applied():
                    for cursor in [cursor1, cursor2]:
                        cursor.execute("SHOW SLAVE STATUS")
                        status = cursor.fetchone()["Slave_SQL_Running_State"]
                        assert status == "Slave has read all relay log; waiting for more updates"

                while_asserts(relay_log_applied, timeout=15)
                cursor1.execute("STOP SLAVE SQL_THREAD")
                cursor2.execute("STOP SLAVE SQL_THREAD")

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

                s3controller = build_controller(
                    default_backup_site=default_backup_site,
                    mysql_config=mysql_empty,
                    session_tmpdir=session_tmpdir,
                )
                s3controller.state_manager.update_state(replication_state={"s1": {}})
                s3controller.binlog_purge_settings["min_binlog_age_before_purge"] = 1
                s3controller.binlog_purge_settings["purge_interval"] = 0.1
                s3controller.start()
                wait_for_condition(lambda: s3controller.state["backups_fetched_at"] != 0, timeout=2)
                backup = s3controller.state["backups"][0]
                s3controller.restore_backup(site=backup["site"], stream_id=backup["stream_id"])

                master_options = {
                    "MASTER_AUTO_POSITION": 1,
                    "MASTER_CONNECT_RETRY": 0.1,
                    "MASTER_HOST": "127.0.0.1",
                    "MASTER_PASSWORD": new_master["password"],
                    "MASTER_PORT": new_master["port"],
                    "MASTER_SSL": 0,
                    "MASTER_USER": master["user"],
                }
                new_mcontroller.switch_to_active_mode()
                change_master_to(cursor=standby_cursor, options=master_options)
                standby_cursor.execute("START SLAVE IO_THREAD, SQL_THREAD")

                # Wait for backup promotion steps to complete
                wait_for_condition(lambda: new_mcontroller.mode == Controller.Mode.active, timeout=15)

                # pylint: disable=protected-access
                promotions = new_mcontroller.backup_streams[0]._get_promotions(ignore_own_promotion=False)
                assert len(promotions) == 2

                new_master_cursor.execute("SET @@GLOBAL.read_only = 0")

                new_master_dg = DataGenerator(
                    connect_info=new_master["connect_options"],
                    index_offset=master_dg.row_count + 1,
                    make_temp_tables=False,
                )
                new_master_dg.start()

                def restore_complete():
                    new_master_cursor.execute("FLUSH BINARY LOGS")
                    return s3controller.restore_coordinator and s3controller.restore_coordinator.is_complete()

                # Wait for replacement server to finish restoring basebackup and whatever binlogs were available
                # when basebackup restoration finished
                wait_for_condition(restore_complete, timeout=120)

                with mysql_cursor(**mysql_empty["connect_options"]) as standby3_cursor:
                    change_master_to(cursor=standby3_cursor, options=master_options)
                    standby3_cursor.execute("START SLAVE IO_THREAD, SQL_THREAD")
                    s3controller.switch_to_observe_mode()

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
                all_old_master_ranges = []
                for binlog in remote_binlogs:
                    ranges = partition_sort_and_combine_gtid_ranges(binlog["gtid_ranges"])
                    old_master_ranges = ranges.get(original_server_uuid)
                    if not old_master_ranges:
                        continue
                    current_range_start = old_master_ranges[0][0]
                    for ranges in all_old_master_ranges:
                        last_range_end = ranges[-1][-1]
                        if current_range_start <= last_range_end:
                            print("Current binlog", binlog, "has range start that predates one of the seen ranges:", ranges)
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
                for server_name, controller in [["s1", s1controller], ["s2", s2controller], ["s3", s3controller]]:
                    with mysql_cursor(**controller.mysql_client_params) as cursor:
                        cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
                        gtid_executed = parse_gtid_range_string(cursor.fetchone()["gtid_executed"])
                        replication_state[server_name] = gtid_executed
                for controller in [s1controller, s2controller, s3controller]:
                    controller.state_manager.update_state(replication_state=replication_state)

                # Set GTID_NEXT on standby to cause new GTID entry getting created even though there are no data
                # changes. This should not interfere with binlog removals.
                with mysql_cursor(**s3controller.mysql_client_params) as cursor:
                    cursor.execute("SELECT @@GLOBAL.server_uuid AS server_uuid")
                    server_uuid = cursor.fetchone()["server_uuid"]
                    cursor.execute(f"SET @@SESSION.GTID_NEXT = '{server_uuid}:1'")
                    cursor.execute("FLUSH BINARY LOGS")
                    cursor.execute(f"SET @@SESSION.GTID_NEXT = '{server_uuid}:2'")
                    cursor.execute("FLUSH BINARY LOGS")
                    time.sleep(0.5)

                def all_binlogs_purged():
                    for controller in [s1controller, s2controller, s3controller]:
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
            s3controller.stop()

    for controller in [mcontroller, s1controller, s2controller, s3controller]:
        assert controller.state["errors"] == 0
        assert controller.backup_streams[0].state["backup_errors"] == 0
        assert controller.backup_streams[0].state["remote_read_errors"] == 0
        assert controller.backup_streams[0].state["remote_write_errors"] == 0


def test_empty_server_backup_and_restore(
    default_backup_site,
    master_controller,
    mysql_empty,
    session_tmpdir,
):
    mcontroller, master = master_controller
    s3controller = None

    mysql_empty["connect_options"]["password"] = master["connect_options"]["password"]

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

        with mysql_cursor(**mysql_empty["connect_options"]) as cursor:
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


def test_multiple_backup_management(master_controller):
    mcontroller, master = master_controller
    # Backup every 3 seconds
    mcontroller.backup_settings["backup_interval_minutes"] = 0.05
    # Never delete backups if we don't have at least 2 no matter how old they are
    mcontroller.backup_settings["backup_count_min"] = 2
    # Delete backups if there are more than this even if the backup to delete is newer than max age
    mcontroller.backup_settings["backup_count_max"] = 20
    # Max age 10 seconds (3-4 backups at any given time)
    mcontroller.backup_settings["backup_age_days_max"] = 1 / 24 / 60 / 6

    mcontroller.switch_to_active_mode()
    mcontroller.start()

    seen_backups = set()
    highest_backup_count = 0
    last_flush = [time.monotonic()]

    def maybe_flush_binlog():
        if time.monotonic() - last_flush[0] > 0.2:
            with mysql_cursor(**master["connect_options"]) as cursor:
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
            # For very brief moments there could be 5 backups
            assert 3 <= len(completed_backups) <= 5
        time.sleep(0.1)

    assert highest_backup_count >= 3

    # We waited for 35 seconds altogether and backup interval is 3 seconds. There should be somewhere between
    # 10 and 13 backups depending on timing
    assert 10 <= len(seen_backups) <= 13


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
    seen_backups = set()
    # Create up to 10 backups so that we have enough to verify deleting backups when max count is exceeded works
    while time.monotonic() - start_time < 30 and len(seen_backups) < 10:
        current_backups = set(backup["stream_id"] for backup in mcontroller.state["backups"] if backup["completed_at"])
        if current_backups - seen_backups:
            mcontroller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)
            seen_backups.update(current_backups)
        time.sleep(0.1)

    assert len(seen_backups) >= 10
    time.sleep(0.1)
    current_backups = set(backup["stream_id"] for backup in mcontroller.state["backups"] if backup["completed_at"])
    assert len(current_backups) == 5


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
    with mysql_cursor(**master["connect_options"]) as cursor:
        cursor.execute("CREATE TABLE test_data (value TEXT)")
        cursor.execute("SHOW BINARY LOGS")
        for binlog in cursor.fetchall():
            initial_binlogs.add(binlog["Log_name"])

    start_time = time.monotonic()
    while time.monotonic() - start_time < 3.5:
        with mysql_cursor(**master["connect_options"]) as cursor:
            cursor.execute("INSERT INTO test_data (value) VALUES (%s)", [str(time.time())])
            cursor.execute("COMMIT")
            cursor.execute("SHOW BINARY LOGS")
            for binlog in cursor.fetchall():
                final_binlogs.add(binlog["Log_name"])

        time.sleep(0.1)

    new_binlogs = final_binlogs - initial_binlogs
    assert len(new_binlogs) == 3

    # Ensure one more rotation is done (we previously wrote some data that was not included in latest rotation)
    mcontroller.rotate_and_back_up_binlog()
    with mysql_cursor(**master["connect_options"]) as cursor:
        cursor.execute("SHOW BINARY LOGS")
        final_binlogs = set(binlog["Log_name"] for binlog in cursor.fetchall())

    new_binlogs = final_binlogs - initial_binlogs
    assert len(new_binlogs) == 4

    # Binlog rotation will happen even if there are no changes
    time.sleep(1.5)
    with mysql_cursor(**master["connect_options"]) as cursor:
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

    while_asserts(has_multiple_backups, timeout=15)
    bb_gtid_executed = mcontroller.backup_streams[-1].state["basebackup_info"]["gtid_executed"]
    assert bb_gtid_executed
    assert bb_gtid_executed == parse_gtid_range_string(gtid_executed)


def test_collect_binlogs_to_purge():
    now = time.time()
    binlogs = [{
        "local_index": 1,
        "gtid_ranges": [{
            "server_uuid": "uuid1",
            "start": 1,
            "end": 6,
        }],
        "processed_at": now - 20,
    }, {
        "local_index": 2,
        "gtid_ranges": [{
            "server_uuid": "uuid1",
            "start": 7,
            "end": 8,
        }],
        "processed_at": now - 10,
    }]
    purge_settings = {
        "min_binlog_age_before_purge": 30,
        "purge_when_observe_no_streams": True,
    }
    log = MagicMock()

    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=None,
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state={},
    )
    assert not binlogs_to_purge
    assert only_binlogs_without_gtids is None
    log.info.assert_called_with(
        "Binlog %s was processed %s seconds ago and min age before purging is %s seconds, not purging", 1, 21, 30
    )

    purge_settings["min_binlog_age_before_purge"] = 5
    bs1 = MagicMock()
    bs1.is_binlog_safe_to_delete.return_value = False
    bs2 = MagicMock()
    bs2.is_binlog_safe_to_delete.return_value = True
    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state={},
    )
    assert not binlogs_to_purge
    assert only_binlogs_without_gtids is None
    log.info.assert_called_with("Binlog %s reported not safe to delete by some backup streams", 1)

    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.observe,
        purge_settings=purge_settings,
        replication_state={},
    )
    assert not binlogs_to_purge
    assert only_binlogs_without_gtids is None
    log.info.assert_called_with(
        "Binlog %s either reported as unsafe to delete (%s) by some stream or not reported as safe to delete by any (%s)", 1,
        True, True
    )

    # No backup streams or replication state and observe node, should allow purging anything
    log = MagicMock()
    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=[],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.observe,
        purge_settings=purge_settings,
        replication_state={},
    )
    assert binlogs_to_purge == binlogs
    assert only_binlogs_without_gtids is None
    log.info.assert_any_call("No backup streams and purging is allowed, assuming purging %s is safe", 1)
    log.info.assert_any_call("No backup streams and purging is allowed, assuming purging %s is safe", 2)
    log.info.assert_any_call("No replication state set, assuming purging binlog %s is safe", 1)
    log.info.assert_any_call("No replication state set, assuming purging binlog %s is safe", 2)

    log = MagicMock()
    replication_state = {
        "server1": {},
    }
    bs1.is_binlog_safe_to_delete.return_value = True
    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert not binlogs_to_purge
    assert only_binlogs_without_gtids is False
    log.info.assert_called_with("Binlog %s not yet replicated to server %r, not purging", 1, "server1")

    log = MagicMock()
    replication_state = {
        "server1": {
            "uuid1": [[1, 7]]
        },
    }
    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert binlogs_to_purge == binlogs[:1]
    assert only_binlogs_without_gtids is False
    log.info.assert_any_call("Binlog %s has been replicated to all servers, purging", 1)
    log.info.assert_any_call("Binlog %s not yet replicated to server %r, not purging", 2, "server1")

    log = MagicMock()
    replication_state = {
        "server1": {
            "uuid1": [[1, 8]]
        },
    }
    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert binlogs_to_purge == binlogs
    assert only_binlogs_without_gtids is False
    log.info.assert_any_call("Binlog %s has been replicated to all servers, purging", 1)
    log.info.assert_any_call("Binlog %s has been replicated to all servers, purging", 2)

    log = MagicMock()
    binlogs[0]["gtid_ranges"] = []
    binlogs[1]["gtid_ranges"] = []
    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert not binlogs_to_purge
    assert only_binlogs_without_gtids is True
    log.info.assert_any_call("Binlog %s has no GTIDs, purging is maybe safe", 1)
    log.info.assert_any_call("Binlog %s has no GTIDs, purging is maybe safe", 2)

    binlogs.append({
        "local_index": 3,
        "gtid_ranges": [{
            "server_uuid": "uuid1",
            "start": 7,
            "end": 8,
        }],
        "processed_at": now - 10,
    })
    log = MagicMock()
    binlogs_to_purge, only_binlogs_without_gtids = Controller.collect_binlogs_to_purge(
        backup_streams=[bs1, bs2],
        binlogs=binlogs,
        log=log,
        mode=Controller.Mode.active,
        purge_settings=purge_settings,
        replication_state=replication_state,
    )
    assert binlogs_to_purge == binlogs
    assert only_binlogs_without_gtids is False
    log.info.assert_any_call("Binlog %s has no GTIDs, purging is maybe safe", 1)
    log.info.assert_any_call("Binlog %s has no GTIDs, purging is maybe safe", 2)
    log.info.assert_any_call("Binlog %s has been replicated to all servers, purging", 3)
