# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import contextlib
import enum
import json
import logging
import multiprocessing
import os
import queue
import threading
import time
from contextlib import suppress

from pghoard.rohmu import errors as rohmu_errors
from pghoard.rohmu import get_transfer

from .append_only_state_manager import AppendOnlyStateManager
from .backup_stream import BINLOG_BUCKET_SIZE
from .basebackup_restore_operation import BasebackupRestoreOperation
from .binlog_downloader import download_binlog
from .state_manager import StateManager
from .util import (
    build_gtid_ranges, change_master_to, make_gtid_range_string, mysql_cursor, parse_fs_metadata, read_gtids_from_log,
    relay_log_name, rsa_decrypt_bytes, sort_and_filter_binlogs, track_rate
)


class RestoreCoordinator(threading.Thread):
    """Restores an existing backup. Starts by restoring the basebackup and then applies
    necessary binlogs on top of that.

    Restoration is performed on a separate thread, which may run further threads, e.g.
    for managing input streams. For downloading, decrypting and decompressing binlogs
    a process pool is used to make sure multiple CPU cores can be utilized for parallel
    processing."""

    # Don't try reading binlogs more often that this if previous call to read binlogs
    # read in all binlogs that were available at that time
    BINLOG_POLL_INTERVAL = 30

    ITERATION_SLEEP_SHORT = 0.2
    ITERATION_SLEEP_LONG = 10

    # If restoring basebackup fails four times for whatever reason, mark this restoration as
    # failed (Phase.failed_basebackup). This is only intended to be triggered in cases where
    # basebackup is corrupt and cannot be restored. Controller will try restoring older basebackup
    # if available (plus binlogs from the backup we were trying to restore) when this happens.
    MAX_BASEBACKUP_ERRORS = 4

    @enum.unique
    class Phase(str, enum.Enum):
        getting_backup_info = "getting_backup_info"
        initiating_binlog_downloads = "initiating_binlog_downloads"
        restoring_basebackup = "restoring_basebackup"
        refreshing_binlogs = "refreshing_binlogs"
        applying_binlogs = "applying_binlogs"
        waiting_for_apply_to_finish = "waiting_for_apply_to_finish"
        finalizing = "finalizing"
        completed = "completed"
        failed = "failed"
        # Terminal state for a RestoreCoordinator instance but restoring an earlier backup may be an option
        failed_basebackup = "failed_basebackup"

    POLL_PHASES = {Phase.waiting_for_apply_to_finish}

    def __init__(
        self,
        *,
        binlog_streams,
        file_storage_config,
        max_binlog_bytes=None,
        mysql_client_params,
        mysql_config_file_name,
        mysql_data_directory,
        mysql_relay_log_index_file,
        mysql_relay_log_prefix,
        pending_binlogs_state_file,
        restart_mysqld_callback,
        rsa_private_key_pem,
        site,
        state_file,
        stats,
        stream_id,
        target_time=None,
        temp_dir,
    ):
        super().__init__()
        self.basebackup_bytes_downloaded = 0
        self.basebackup_restore_operation = None
        self.binlog_poll_interval = self.BINLOG_POLL_INTERVAL
        # Binary logs may be fetched from multiple consecutive backup streams. This is utilized if restoring
        # a basebackup fails for any reason but earlier backups are available and basebackup from one of those
        # can be successfully restored.
        self.binlog_streams = binlog_streams
        self.current_file = None
        self.file_storage = None
        self.file_storage_config = file_storage_config
        self.is_running = True
        self.iteration_sleep_long = self.ITERATION_SLEEP_LONG
        self.iteration_sleep_short = self.ITERATION_SLEEP_SHORT
        self.lock = threading.RLock()
        self.log = logging.getLogger(f"{self.__class__.__name__}/{stream_id}")
        self.max_binlog_count = None
        # Maximum bytes worth of binlogs to store on disk simultaneously. Note that this is
        # not an actual upper limit as the constraint is checked after adding new binlog
        # (or else it might be possible no binlogs can be downloaded)
        self.max_binlog_bytes = max_binlog_bytes
        self.mp_context = multiprocessing.get_context("spawn")
        self.mysql_client_params = mysql_client_params
        self.mysql_config_file_name = mysql_config_file_name
        self.mysql_data_directory = mysql_data_directory
        self.mysql_relay_log_index_file = mysql_relay_log_index_file
        self.mysql_relay_log_prefix = mysql_relay_log_prefix
        self.ongoing_prefetch_operations = {}
        # Number of pending binlogs can be potentially very large. Store those to separate file to avoid
        # the frequently updated main state growing so large that saving it causes noticeable overhead
        pending_binlogs = []
        self.pending_binlog_manager = AppendOnlyStateManager(
            entries=pending_binlogs, lock=self.lock, state_file=pending_binlogs_state_file
        )
        self.pending_binlogs = pending_binlogs
        self.queue_in = self.mp_context.Queue()
        self.queue_out = self.mp_context.Queue()
        self.restart_mysqld_callback = restart_mysqld_callback
        if not isinstance(rsa_private_key_pem, bytes):
            rsa_private_key_pem = rsa_private_key_pem.encode("ascii")
        self.rsa_private_key_pem = rsa_private_key_pem
        self.site = site
        # State contains variables that should be persisted over process restart
        # so that the operation resumes from where it was left (whenever possible)
        self.state = {
            "applying_binlogs": [],
            "binlogs_picked_for_apply": 0,
            "basebackup_info": {},
            "basebackup_restore_duration": None,
            "basebackup_restore_errors": 0,
            "binlog_name_offset": 0,
            "binlog_stream_offset": 0,
            "binlogs_restored": 0,
            "completed_apply_ops": [],
            "completed_info": None,
            "current_binlog_bucket": 0,
            "current_binlog_stream_index": 0,
            "current_executed_gtid_target": {},
            "current_relay_log_target": None,
            # This is required so that we can correctly update pending_binlogs state file if updating that
            # fails after the main state has already been updated
            "expected_first_pending_binlog_remote_index": None,
            "file_fail_counters": {},
            "last_poll": None,
            "last_processed_index": None,
            "mysql_params": None,
            "phase": self.Phase.getting_backup_info,
            "prefetched_binlogs": {},
            "promotions": [],
            "remote_read_errors": 0,
            "restore_errors": 0,
            "server_uuid": None,
            "target_time_reached": False,
        }
        self.state_manager = StateManager(lock=self.lock, state=self.state, state_file=state_file)
        self.stats = stats
        self.stream_id = stream_id
        self.target_time = target_time
        self.temp_dir = temp_dir
        self.worker_processes = []

    def add_new_binlog_streams(self, new_binlog_streams):
        if not self.can_add_binlog_streams():
            return False
        self.binlog_streams = self.binlog_streams + new_binlog_streams
        return True

    @property
    def basebackup_bytes_total(self):
        return self.state["basebackup_info"].get("compressed_size") or 0

    @property
    def binlogs_being_restored(self):
        return len(self.state["applying_binlogs"] or [])

    @property
    def binlogs_pending(self):
        with self.lock:
            return len(self.pending_binlogs)

    @property
    def binlogs_restored(self):
        return self.state["binlogs_restored"]

    def can_add_binlog_streams(self):
        # If we're restoring to a specific backup then we don't want to look for possible new backup
        # streams that we should restore. Also, if we've already decided to stop looking for binlogs
        # cannot add new ones or if we're already past the point of applying binlogs altogether we
        # obviously cannot do anything with new binlog streams.
        final_phases = {self.Phase.finalizing, self.Phase.completed, self.Phase.failed, self.Phase.failed_basebackup}
        return not self.target_time and not self.state["target_time_reached"] and self.phase not in final_phases

    def is_complete(self):
        return self.phase == self.Phase.completed

    @property
    def phase(self):
        return self.state["phase"]

    def run(self):
        self.log.info("Restore coordinator running")
        self._start_process_pool()
        # If we're in a state where binary logs should be downloaded ensure we have appropriate
        # download operations scheduled. If restore coordinator is destroyed and new one re-created
        # in the middle of applying binary logs we could end up not having any ongoing download
        # operations for the newly created restore coordinator, causing restoration to stall
        self._queue_prefetch_operations()

        while self.is_running:
            try:
                if not self.file_storage:
                    self.log.info("Creating file storage accessor")
                    self.file_storage = get_transfer(self.file_storage_config)

                if self.phase == self.Phase.getting_backup_info:
                    self.get_backup_info()
                if self.phase == self.Phase.initiating_binlog_downloads:
                    self.initiate_binlog_downloads()
                if self.phase == self.Phase.restoring_basebackup:
                    self.restore_basebackup()
                if self.phase == self.Phase.refreshing_binlogs:
                    self.refresh_binlogs()
                if self.phase == self.Phase.applying_binlogs:
                    self.apply_binlogs()
                if self.phase == self.Phase.waiting_for_apply_to_finish:
                    if self.wait_for_apply_to_finish():
                        continue
                if self.phase == self.Phase.finalizing:
                    self.finalize_restoration()
                if self.phase in {self.Phase.completed, self.Phase.failed, self.Phase.failed_basebackup}:
                    break
                # Blocks for up to self._get_iteration_sleep() seconds if there are no events in queue
                self.read_queue()
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unexpected exception while restoring backup")
                self.stats.unexpected_exception(ex=ex, where="RestoreCoordinator.run")
                self.state_manager.increment_counter(name="restore_errors")
                self.stats.increase("myhoard.restore_errors")
                time.sleep(min(self._get_iteration_sleep(), 2))
        self.is_running = False

    @property
    def server_uuid(self):
        return self.state["server_uuid"]

    def stop(self):
        self.log.info("Stopping restore coordinator")
        self.is_running = False
        self.queue_in.put(None)
        for _ in range(len(self.worker_processes)):
            self.queue_out.put(None)
        # Thread might not have been started or could've already been joined, we don't care about that
        with suppress(Exception):
            self.join()
        for worker in self.worker_processes:
            worker.join()
        self.worker_processes = []
        self.log.info("Restore coordinator stopped")

    def get_backup_info(self):
        if not self.state["completed_info"]:
            completed_info = self._load_file_data("completed.json")
            if not completed_info:
                self.log.error("Backup is not complete, cannot restore")
                self.state_manager.increment_counter(name="restore_errors")
                return
            self.update_state(completed_info=completed_info)

        basebackup_info = self._load_file_data("basebackup.json")
        if not basebackup_info:
            return
        self.update_state(
            basebackup_info=basebackup_info,
            phase=self.Phase.initiating_binlog_downloads,
        )

    def initiate_binlog_downloads(self):
        self._fetch_more_binlog_infos()
        self.update_state(phase=self.Phase.restoring_basebackup)

    def restore_basebackup(self):
        start_time = time.monotonic()
        encryption_key = rsa_decrypt_bytes(
            self.rsa_private_key_pem, bytes.fromhex(self.state["basebackup_info"]["encryption_key"])
        )
        self.basebackup_restore_operation = BasebackupRestoreOperation(
            encryption_algorithm="AES256",
            encryption_key=encryption_key,
            mysql_config_file_name=self.mysql_config_file_name,
            mysql_data_directory=self.mysql_data_directory,
            stats=self.stats,
            stream_handler=self._basebackup_data_provider,
            temp_dir=self.temp_dir,
        )
        try:
            self.basebackup_restore_operation.restore_backup()
            duration = time.monotonic() - start_time
            self.log.info("Basebackup restored in %.2f seconds", duration)
            self.update_state(
                phase=self.Phase.refreshing_binlogs,
                basebackup_restore_duration=duration,
            )
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Failed to restore basebackup: %r", ex)
            self.stats.unexpected_exception(ex=ex, where="RestoreCoordinator.restore_basebackup")
            self.state_manager.increment_counter(name="basebackup_restore_errors")
            self.state_manager.increment_counter(name="restore_errors")
            self.stats.increase("myhoard.restore_errors")
            if self.state["basebackup_restore_errors"] >= self.MAX_BASEBACKUP_ERRORS:
                self.log.error(
                    "Restoring basebackup failed %s times, assuming the backup is broken", self.MAX_BASEBACKUP_ERRORS
                )
                self.update_state(phase=self.Phase.failed_basebackup)
                self.stats.increase("myhoard.basebackup_broken")
        finally:
            self.basebackup_restore_operation = None

    def refresh_binlogs(self):
        self._fetch_more_binlog_infos(force=True)
        if not self.pending_binlogs:
            self.log.info("No binary logs available, marking restore completed immediately")
            self.update_state(phase=self.Phase.finalizing)
        else:
            self.update_state(completed_apply_ops=[], phase=self.Phase.applying_binlogs)

    def apply_binlogs(self):
        binlogs = self._get_binlogs_to_apply()
        if not binlogs:
            return

        names = [
            self._relay_log_name(index=binlog["adjusted_remote_index"] + self.state["binlog_name_offset"], full_path=False)
            for binlog in binlogs
        ]

        last_range = None
        for binlog in binlogs:
            if binlog["gtid_ranges"]:
                last_range = binlog["gtid_ranges"][-1]
        last_remote_index = binlogs[-1]["adjusted_remote_index"]
        relay_log_target = last_remote_index + self.state["binlog_name_offset"] + 1

        initial_round = binlogs[0]["adjusted_remote_index"] == 1
        final_round = binlogs[-1]["adjusted_remote_index"] == self.pending_binlogs[-1]["adjusted_remote_index"]
        if initial_round:
            self._rename_prefetched_binlogs(binlogs)
            with open(self.mysql_relay_log_index_file, "wb") as index_file:
                self.log.info("Writing relay log names from %r to %r", names[0], names[-1])
                # File must end with linefeed or else last line will not be processed correctly
                index_file.write(("\n".join(names) + "\n").encode("utf-8"))
            self._patch_gtid_executed(binlogs[0])

        all_gtids_applied = False
        until_after_gtids = None
        if final_round and self.target_time and binlogs[0]["gtid_ranges"]:
            renamed = "rename" in self.state["completed_apply_ops"]
            if renamed:
                file_name = self._relay_log_name(
                    index=binlogs[0]["adjusted_remote_index"] + self.state["binlog_name_offset"]
                )
            else:
                file_name = self._relay_log_prefetch_name(index=binlogs[0]["adjusted_remote_index"])
            ranges = list(build_gtid_ranges(read_gtids_from_log(file_name, read_until_time=self.target_time)))
            if ranges:
                last_range = ranges[-1]
                until_after_gtids = "{}:{}".format(last_range["server_uuid"], last_range["end"])
                # Don't expect any specific file because if the GTID we're including is the very last entry
                # in the file the SQL thread might switch to next file and if it is earlier then it won't
                # so we'd need to be watching for two file names. Because execution is always single threaded
                # checking just the commit should be sufficient anyway
                relay_log_target = None
                self.log.info("Restoring up to and including target GTID %r", until_after_gtids)
            else:
                self.log.info("No GTID ranges found in last file with given target timestamp, finalizing restore")
                all_gtids_applied = True

        self._ensure_mysql_server_is_started(with_binlog=False, with_gtids=True)

        if not all_gtids_applied:
            with self._mysql_cursor() as cursor:
                if not initial_round:
                    self._generate_updated_relay_log_index(binlogs, cursor)

                # Currently always use single threaded processing. Multithreading would be potentially much faster but
                # there are some issues that would need to be resolved before that can properly be used:
                # 1. Multithreading cannot be used with UNTIL SQL_AFTER_GTIDS option. This is not a major issue as
                #    the system automatically switches to single threaded mode when that option is used.
                # 2. Using CHANGE MASTER TO (after master info has been previously set) to set position to a relay log
                #    file which does not end at an mts recovery group boundary (and there's no subsequent file which
                #    would have that boundary either) fails when using multithreading. Trying to explicitly switch to
                #    single threaded mode when encountering this error does not seem to help (but using single
                #    threaded mode from get go does). Error in MySQL server logs is
                #    "Error looking for file after <relay log name>" and error we get back on client side is
                #    pymysql.err.InternalError with args[0] == 1201
                # 3. Using START SLAVE for the first time with multithreading enabled stalls, presumably if there isn't
                #    a single mts recovery group in the list of relay logs that have been specified.
                if initial_round:
                    cursor.execute(f"SET @@GLOBAL.slave_parallel_workers = 0")

                # Start from where basebackup ended for first binlog and for later iterations after file magic bytes
                relay_log_pos = (self.state["basebackup_info"]["binlog_position"] or 4) if initial_round else 4
                self.log.info("Changing master position to %s in file %s", relay_log_pos, names[0])
                change_master_to(
                    cursor=cursor,
                    options={
                        "MASTER_AUTO_POSITION": 0,
                        "MASTER_HOST": "dummy",
                        "RELAY_LOG_FILE": names[0],
                        "RELAY_LOG_POS": relay_log_pos,
                    },
                )
                sql = "START SLAVE SQL_THREAD"
                if until_after_gtids:
                    sql += f" UNTIL SQL_AFTER_GTIDS = '{until_after_gtids}'"
                cursor.execute(sql)

        prefetched_binlogs = self.state["prefetched_binlogs"]
        for binlog in binlogs:
            del prefetched_binlogs[binlog["remote_key"]]
        pending_binlogs = self.pending_binlogs[len(binlogs):]
        # Mark target_time_reached as True if we started applying the last binlog whose info we had previously
        # fetched to avoid more binlogs being retrieved in case we're syncing against active master
        target_time_reached = self.state["target_time_reached"] or not pending_binlogs

        applying_binlogs = []
        for binlog in binlogs:
            applying_binlogs.append({
                "adjusted_index": binlog["adjusted_remote_index"] + self.state["binlog_name_offset"],
                "file_size": binlog["file_size"],
            })
        if all_gtids_applied:
            applying_binlogs = []

        with self.lock:
            if pending_binlogs:
                expected_first_pending_binlog_remote_index = pending_binlogs[0]["adjusted_remote_index"]
            else:
                expected_first_pending_binlog_remote_index = None
            self.update_state(
                applying_binlogs=applying_binlogs,
                binlogs_picked_for_apply=0,
                current_executed_gtid_target=last_range,
                current_relay_log_target=relay_log_target,
                expected_first_pending_binlog_remote_index=expected_first_pending_binlog_remote_index,
                phase=self.Phase.finalizing if all_gtids_applied else self.Phase.waiting_for_apply_to_finish,
                prefetched_binlogs=prefetched_binlogs,
                target_time_reached=target_time_reached,
            )
            self.pending_binlog_manager.remove_many_from_head(len(binlogs))

    def wait_for_apply_to_finish(self):
        expected_first_index = self.state["expected_first_pending_binlog_remote_index"]
        if expected_first_index:
            count_to_drop = 0
            for binlog in self.pending_binlogs:
                if binlog["adjusted_remote_index"] < expected_first_index:
                    count_to_drop += 1
                else:
                    break
            if count_to_drop > 0:
                self.pending_binlog_manager.remove_many_from_head(count_to_drop)
            self.state_manager.update_state(expected_first_pending_binlog_remote_index=None)

        self._fetch_more_binlog_infos()

        apply_finished, current_index = self._check_sql_slave_status()
        applying_binlogs = self.state["applying_binlogs"]
        applied_binlog_count = 0
        for binlog in applying_binlogs:
            if binlog["adjusted_index"] >= current_index:
                break
            applied_binlog_count += 1

        if applied_binlog_count > 0:
            applying_binlogs = applying_binlogs[applied_binlog_count:]
            self.update_state(
                applying_binlogs=applying_binlogs,
                binlogs_restored=self.binlogs_restored + applied_binlog_count,
            )
            self.stats.increase("myhoard.restore.binlogs_restored", applied_binlog_count)
            self._queue_prefetch_operations()
        if apply_finished:
            if not self.target_time or self.pending_binlogs:
                # Should not happen, here to catch programming errors
                assert not applying_binlogs, f"Some binlogs remained in {applying_binlogs!r} after completion"
            if self.pending_binlogs:
                phase = self.Phase.applying_binlogs
            else:
                self.log.info("Applied all pending binlogs, changing phase to 'finalizing'")
                phase = self.Phase.finalizing
            # Sometimes unexpected extra relay log files are generated. Take that into account when generating new
            # names so that we keep on creating the files with correct names
            offset = self.state["binlog_name_offset"]
            target_index = self.state["current_relay_log_target"]
            if target_index is not None and current_index > target_index:
                self.log.warning("Expected to reach binlog index %r but reached %r instead", target_index, current_index)
                self.stats.increase("myhoard.restore.unexpected_extra_relay_log")
                offset += (current_index - target_index)
            self.update_state(binlog_name_offset=offset, completed_apply_ops=[], phase=phase)
        return apply_finished

    def finalize_restoration(self):
        # If there were no binary logs to restore MySQL server has not been started yet and trying
        # to connect to it would fail. If it hasn't been started (no mysql_params specified) it also
        # doesn't have slave configured or running so we can just skip the calls below.
        if self.state["mysql_params"]:
            with self._mysql_cursor() as cursor:
                cursor.execute("STOP SLAVE")
                cursor.execute("CHANGE MASTER TO MASTER_HOST='dummy'")
        self._ensure_mysql_server_is_started(with_binlog=True, with_gtids=True)
        self.update_state(phase=self.Phase.completed)
        self.log.info("Backup restoration completed")

    def read_queue(self):
        try:
            result = self.queue_in.get(timeout=self._get_iteration_sleep())
            # Empty results may be posted to wake up the thread
            if not result:
                return
            self._process_work_queue_result(result)
            while True:
                try:
                    result = self.queue_in.get(block=False)
                    if result:
                        self._process_work_queue_result(result)
                except queue.Empty:
                    break
        except queue.Empty:
            pass

    def update_state(self, **kwargs):
        self.state_manager.update_state(**kwargs)

    def _process_work_queue_result(self, result):
        key = result["remote_key"]
        binlog = self.ongoing_prefetch_operations.pop(key)
        fail_counters = self.state["file_fail_counters"]
        if result["result"] == "success":
            fail_counters.pop(key, None)
            self.log.info("Successfully prefetched %r", key)
            prefetched_binlogs = self.state["prefetched_binlogs"]
            # TODO: Add some tracking for how long has elapsed since we got any results from
            # downloaders and if enough time has passed tear down processes, create new queues,
            # clear `ongoing_prefetch_operations`, restart processes and put download items back to queue
            self.update_state(
                file_fail_counters=fail_counters,
                prefetched_binlogs={
                    **prefetched_binlogs, key: binlog["file_size"]
                },
            )
        else:
            fail_counters[key] = fail_counters.get(key, 0) + 1
            retry = fail_counters[key] < 3
            if retry:
                self.log.error("Failed to fetch %r: %r. Retrying", key, result["message"])
                result.pop("result")
                result.pop("message")
                self.queue_out.put(result)
                self.ongoing_prefetch_operations[key] = binlog
                self.update_state(file_fail_counters=fail_counters)
            else:
                self.log.error(
                    "Failed to fetch %r: %r. Too many (%s) failures, marking restoration as failed", key, result["message"],
                    fail_counters[key]
                )
                self.update_state(file_fail_counters=fail_counters, phase=self.Phase.failed)

    def _build_binlog_full_name(self, name):
        binlog_stream = self.binlog_streams[self.state["current_binlog_stream_index"]]
        site = binlog_stream["site"]
        stream_id = binlog_stream["stream_id"]
        return f"{site}/{stream_id}/{name}"

    def _build_full_name(self, name):
        return f"{self.site}/{self.stream_id}/{name}"

    def _load_file_data(self, name, missing_ok=False):
        try:
            info_str, _ = self.file_storage.get_contents_to_string(self._build_full_name(name))
            return json.loads(info_str)
        except rohmu_errors.FileNotFoundFromStorageError as ex:
            if not missing_ok:
                self.log.error("File %r not found from storage", name)
                self.stats.unexpected_exception(ex=ex, where="RestoreCoordinator._load_file_data")
                self.state_manager.increment_counter(name="remote_read_errors")
                self.stats.increase("myhoard.restore_errors")
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Downloading file %r failed", name)
            self.stats.unexpected_exception(ex=ex, where="RestoreCoordinator._load_file_data")
            self.state_manager.increment_counter(name="remote_read_errors")
            self.stats.increase("myhoard.remote_read_errors")
        return None

    def _basebackup_data_provider(self, target_stream):
        name = self._build_full_name("basebackup.xbstream")
        compressed_size = self.state["basebackup_info"].get("compressed_size")
        file_storage = get_transfer(self.file_storage_config)

        last_time = [time.monotonic()]
        last_value = [0]
        self.basebackup_bytes_downloaded = 0

        def download_progress(progress, max_progress):
            if progress and max_progress and compressed_size:
                # progress may be the actual number of bytes or it may be percentages
                self.basebackup_bytes_downloaded = int(compressed_size * progress / max_progress)
                # Track both absolute number and explicitly calculated rate. The rate can be useful as
                # a separate measurement because downloads are not ongoing all the time and calculating
                # rate based on raw byte counter requires knowing when the operation started and ended
                self.stats.gauge_int("myhoard.restore.basebackup_bytes_downloaded", self.basebackup_bytes_downloaded)
                last_value[0], last_time[0] = track_rate(
                    current=self.basebackup_bytes_downloaded,
                    last_recorded=last_value[0],
                    last_recorded_time=last_time[0],
                    metric_name="myhoard.restore.basebackup_download_rate",
                    stats=self.stats,
                )

        file_storage.get_contents_to_fileobj(name, target_stream, progress_callback=download_progress)

    def _get_iteration_sleep(self):
        if self.phase in self.POLL_PHASES:
            return self.iteration_sleep_short
        else:
            return self.iteration_sleep_long

    @staticmethod
    def _get_sorted_file_infos(infos):
        def build_sort_key(info):
            # name is path/index_server, e.g. 2019-01-12T07:43:20Z_7fba6afa-83f8-43e5-a565-0c6ab43386af/binlogs/0/100_2,
            # get the index part (100) as integer
            name = info["name"].rsplit("/", 1)[-1]
            index = name.split("_", 1)[0]
            return int(index)

        return sorted(infos, key=build_sort_key)

    def _list_binlogs_in_bucket(self, bucket):
        last_processed_index = self.state["last_processed_index"]
        new_binlogs = []
        highest_index = 0
        start_time = time.monotonic()
        target_time_reached_by_server = set()

        self.log.debug("Listing binlogs in bucket %s", bucket)
        try:
            list_iter = self.file_storage.list_iter(self._build_binlog_full_name(f"binlogs/{bucket}"))
            for info in self._get_sorted_file_infos(list_iter):
                binlog = parse_fs_metadata(info["metadata"])
                # We may be handling binlogs from multiple streams. To make the other logic work, calculate
                # monotonically increasing index across all streams. (Individual streams have their indexes
                # always start from 1.)
                binlog["adjusted_remote_index"] = self.state["binlog_stream_offset"] + binlog["remote_index"]
                binlog["remote_key"] = info["name"]
                binlog["remote_size"] = info["size"]
                highest_index = max(highest_index, binlog["remote_index"])
                if last_processed_index is not None and binlog["adjusted_remote_index"] <= last_processed_index:
                    continue
                # We're handing binlogs in order. If we've reached target time for any earlier binlog then this
                # binlog must be out of range as well. This check is needed because we might have binlogs without
                # GTIDs that cannot be excluded based on start/end checks
                if binlog["server_id"] in target_time_reached_by_server:
                    continue
                if self.target_time and binlog["gtid_ranges"]:
                    if binlog["gtid_ranges"][0]["start_ts"] >= self.target_time:
                        # We exclude entries whose time matches recovery target time so any file whose start_ts
                        # is equal or higher than target time is certain not to contain data we're going to apply
                        self.log.info(
                            "Start time %s of binlog %s from server %s is after our target time %s, skipping",
                            binlog["gtid_ranges"][0]["start_ts"], binlog["remote_index"], binlog["server_id"],
                            self.target_time
                        )
                        target_time_reached_by_server.add(binlog["server_id"])
                        continue
                    if binlog["gtid_ranges"][0]["end_ts"] >= self.target_time:
                        # Log and mark target time reached but include binlog and continue processing results. We may
                        # get binlogs from multiple servers in some race conditions and we don't yet know if this binlog
                        # was from a server that was actually valid at that point in time and some other server may have
                        # binlogs that are still relevant.
                        self.log.info(
                            "End time %s of binlog %s from server %s is at or after our target time %s, target time reached",
                            binlog["gtid_ranges"][0]["end_ts"], binlog["remote_index"], binlog["server_id"], self.target_time
                        )
                        target_time_reached_by_server.add(binlog["server_id"])
                new_binlogs.append(binlog)
        except rohmu_errors.FileNotFoundFromStorageError:
            pass
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error("Failed to list remote binlogs: %r", ex)
            self.stats.unexpected_exception(ex=ex, where="RestoreCoordinator._load_file_data")
            self.state_manager.increment_counter(name="remote_read_errors")
            self.stats.increase("myhoard.remote_read_errors")
            return None, None, None

        duration = time.monotonic() - start_time
        self.log.info("Found %s binlogs from bucket %s in %.2f seconds", len(new_binlogs), bucket, duration)
        return new_binlogs, highest_index, bool(target_time_reached_by_server)

    # pylint: disable=too-many-return-statements
    def _fetch_more_binlog_infos(self, force=False):
        boundary_position = False
        if self.pending_binlogs:
            boundary_position = (self.pending_binlogs[-1]["remote_index"] + 1) % BINLOG_BUCKET_SIZE == 0
        if self.state["target_time_reached"] or len(self.pending_binlogs) > 200 or (
            not boundary_position and self.state["last_poll"]
            and time.time() - self.state["last_poll"] < self.binlog_poll_interval and not force
        ):
            # If we've consumed binlogs that were available for current stream switch to next if available
            if (
                len(self.pending_binlogs) == 1 and not self.state["target_time_reached"] and
                self._switch_to_next_binlog_stream()
            ):
                self._fetch_more_binlogs_infos_for_current_stream()
            return

        self._fetch_more_binlogs_infos_for_current_stream()
        # If we've consumed binlogs that were available for current stream switch to next if available
        if (
            len(self.pending_binlogs) == 1 and not self.state["target_time_reached"] and
            self._switch_to_next_binlog_stream()
        ):
            self._fetch_more_binlogs_infos_for_current_stream()

    def _fetch_more_binlogs_infos_for_current_stream(self):
        bucket = self.state["current_binlog_bucket"]
        new_binlogs = []
        while True:
            previous_bucket = bucket
            binlogs, highest_index, target_time_reached = self._list_binlogs_in_bucket(bucket)
            if binlogs is None:
                return

            # Move to next bucket of BINLOG_BUCKET_SIZE binlogs if the listing contained last binlog that
            # is expected to be found from current bucket
            if (highest_index + 1) % BINLOG_BUCKET_SIZE == 0:
                bucket += 1

            new_binlogs.extend(binlogs)

            # Fetch more entries from next bucket if we only have 0 (shouldn't happen) or 1 binlog so
            # that the logic that determines whether we've reached the end of binlogs doesn't kick in
            if target_time_reached or len(binlogs) > 1 or previous_bucket == bucket:
                break

        if not new_binlogs:
            self.update_state(
                current_binlog_bucket=bucket,
                last_poll=time.time(),
                target_time_reached=target_time_reached,
            )
            return

        # Also refresh promotions list so that we know which of the remote
        # binlogs are actually valid
        promotions = {}
        try:
            for info in self.file_storage.list_iter(self._build_binlog_full_name("promotions")):
                # There could theoretically be multiple promotions with the same
                # index value if new master got promoted but then failed before
                # managing to upload any binlogs. To cope with that only keep one
                # promotion info per server id (the one with most recent timestamp)
                info = parse_fs_metadata(info["metadata"])
                existing = promotions.get(info["start_index"])
                if existing and info["promoted_at"] < existing["promoted_at"]:
                    continue
                promotions[info["start_index"]] = info
                self.log.info(
                    "server_id %s valid starting from %s (at %s)",
                    info["server_id"],
                    info["start_index"],
                    info["promoted_at"],
                )
        except Exception as ex:  # pylint: disable=broad-except
            # There should always be one promotion file so file not found is real error too
            self.log.error("Failed to list promotions: %r", ex)
            self.stats.unexpected_exception(ex=ex, where="RestoreCoordinator._fetch_more_binlog_infos")
            self.state_manager.increment_counter(name="remote_read_errors")
            self.stats.increase("myhoard.remote_read_errors")
            return

        if 1 not in promotions:
            self.state_manager.increment_counter(name="restore_errors")
            self.log.error("Missing initial promotion info: %r", promotions)
            return

        promotions = {start_index: info["server_id"] for start_index, info in promotions.items()}
        last_index = 0 if not self.pending_binlogs else self.pending_binlogs[-1]["adjusted_remote_index"]
        try:
            new_binlogs = sort_and_filter_binlogs(
                binlogs=new_binlogs, log=self.log, last_index=last_index, promotions=promotions
            )
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error("Sorting and filtering binlogs failed: %r", ex)
            self.stats.unexpected_exception(ex=ex, where="RestoreCoordinator._fetch_more_binlog_infos")
            self.state_manager.increment_counter(name="restore_errors")
            self.stats.increase("myhoard.restore_errors")
            return

        if target_time_reached:
            # If list of binlogs ends with binlogs that don't have any GTIDs exclude those from the actual
            # list we're going to process. Later logic assumes that only the last binlog we're processing
            # may be at the target time boundary and having binlogs with no GTIDs at the end of the list
            # makes that logic fail. Binlogs with no GTIDs should be empty anyway so excluding them should
            # have no ill effect.
            while new_binlogs and not new_binlogs[-1]["gtid_ranges"]:
                self.log.info(
                    "Dropping last new binlog %r because target time is reached and binlog is empty",
                    new_binlogs[-1]["remote_index"]
                )
                new_binlogs.pop()

        if not new_binlogs:
            self.update_state(
                current_binlog_bucket=bucket,
                last_poll=time.time(),
                target_time_reached=target_time_reached,
            )
            return

        # If persisting new binlogs succeeding but persisting other state failed we might get same binlogs anew
        actual_new_binlogs = []
        last_existing_index = self.pending_binlogs[-1]["adjusted_remote_index"] if self.pending_binlogs else None
        for binlog in new_binlogs:
            if not last_existing_index or binlog["adjusted_remote_index"] > last_existing_index:
                actual_new_binlogs.append(binlog)

        with self.lock:
            self.pending_binlog_manager.append_many(actual_new_binlogs)
            if new_binlogs:
                last_processed_index = new_binlogs[-1]["adjusted_remote_index"]
            else:
                last_processed_index = self.state["last_processed_index"]
            self.update_state(
                current_binlog_bucket=bucket,
                last_poll=time.time(),
                last_processed_index=last_processed_index,
                target_time_reached=target_time_reached,
            )

        self._queue_prefetch_operations()
        self.stats.gauge_int("myhoard.restore.pending_binlogs", len(self.pending_binlogs))

    def _queue_prefetch_operations(self):
        on_disk_binlog_count = (
            len(self.ongoing_prefetch_operations) + len(self.state["prefetched_binlogs"]) +
            len(self.state["applying_binlogs"])
        )
        ongoing_bytes = sum(binlog["file_size"] for binlog in self.ongoing_prefetch_operations.values())
        prefetched_bytes = sum(self.state["prefetched_binlogs"].values())
        applying_bytes = sum(binlog["file_size"] for binlog in self.state["applying_binlogs"])
        on_disk_binlog_bytes = ongoing_bytes + prefetched_bytes + applying_bytes
        for binlog in self.pending_binlogs:
            if self.max_binlog_count and on_disk_binlog_count >= self.max_binlog_count:
                break
            if self.max_binlog_bytes and on_disk_binlog_bytes >= self.max_binlog_bytes:
                break

            key = binlog["remote_key"]
            if key in self.ongoing_prefetch_operations or key in self.state["prefetched_binlogs"]:
                continue
            self.ongoing_prefetch_operations[key] = binlog
            props = {
                "compression_algorithm": binlog["compression_algorithm"],
                "remote_file_size": binlog["remote_size"],
                "local_file_name": self._relay_log_prefetch_name(index=binlog["adjusted_remote_index"]),
                "remote_key": key,
            }
            self.log.info("Queuing prefetch operation for %r", key)
            self.queue_out.put(props)
            on_disk_binlog_count += 1
            on_disk_binlog_bytes += binlog["file_size"]

    @contextlib.contextmanager
    def _mysql_cursor(self):
        with mysql_cursor(
            host=self.mysql_client_params["host"],
            password=self.mysql_client_params["password"],
            port=self.mysql_client_params["port"],
            user=self.mysql_client_params["user"],
        ) as cursor:
            yield cursor

    def _parse_gtid_executed_ranges(self, binlog):
        if not binlog["gtid_ranges"] or not self.state["basebackup_info"]["binlog_position"]:
            return []

        # Scan all GTIDs before our start location and update server gtid_executed based on that.
        # xtrabackup does not enforce log rotation when it takes the backup and the actual executed
        # GTIDs may only be cached in memory and will not appear on this node because memory cache is
        # only guaranteed to be flushed on log rotation. MySQL server normally recovers by parsing
        # binlogs to see what has actually been executed but since we don't have a binlog in correct
        # state and generating one is cumbersome as well, manually update the table via SQL later.
        local_name = self._relay_log_name(index=binlog["adjusted_remote_index"] + self.state["binlog_name_offset"])
        return list(
            build_gtid_ranges(
                read_gtids_from_log(local_name, read_until_position=self.state["basebackup_info"]["binlog_position"])
            )
        )

    def _get_binlogs_to_apply(self):
        binlogs = []
        binlogs_picked_for_apply = self.state["binlogs_picked_for_apply"]

        for idx, binlog in enumerate(self.pending_binlogs):
            # Always process last binlog separately because we need to disable multithreaded
            # processing for that in case there is explicit target time that should be reached
            # because multithreaded processing cannot be used together with the UNTIL options
            if binlogs_picked_for_apply > 0:
                if idx < binlogs_picked_for_apply:
                    binlogs.append(binlog)
                else:
                    break
            elif idx + 1 == len(self.pending_binlogs) and binlogs:
                break
            elif binlog["remote_key"] in self.state["prefetched_binlogs"]:
                binlogs.append(binlog)
            else:
                break

        # Nothing available yet
        if not binlogs:
            return None

        self.update_state(binlogs_picked_for_apply=len(binlogs))
        return binlogs

    def _generate_updated_relay_log_index(self, binlogs, cursor):
        # Should already be stopped but just to make sure
        cursor.execute("STOP SLAVE")
        cursor.execute("SHOW SLAVE STATUS")
        initial_relay_log_file = cursor.fetchone()["Relay_Log_File"]

        # Technically we'd want one fewer relay log file here but the server seems to have some
        # caching logic related to the current relay log and we need to make sure currently active
        # log is after the last log we want to replay to ensure all logs get applied
        if "flush_relay_logs" not in self.state["completed_apply_ops"]:
            for _ in binlogs:
                cursor.execute("FLUSH RELAY LOGS")
            self.update_state(completed_apply_ops=self.state["completed_apply_ops"] + ["flush_relay_logs"])
            cursor.execute("SHOW SLAVE STATUS")
            final_relay_log_file = cursor.fetchone()["Relay_Log_File"]
            self.log.info(
                "Flushed relay logs %s times, initial file was %r and current is %r", len(binlogs), initial_relay_log_file,
                final_relay_log_file
            )

        self._rename_prefetched_binlogs(binlogs)

    def _rename_prefetched_binlogs(self, binlogs):
        if "rename" in self.state["completed_apply_ops"]:
            return
        for binlog in binlogs:
            local_prefetch_name = self._relay_log_prefetch_name(index=binlog["adjusted_remote_index"])
            if os.path.exists(local_prefetch_name):
                local_name = self._relay_log_name(index=binlog["adjusted_remote_index"] + self.state["binlog_name_offset"])
                os.rename(local_prefetch_name, local_name)
                self.log.info("Renamed %s to %s", local_prefetch_name, local_name)
        self.update_state(completed_apply_ops=self.state["completed_apply_ops"] + ["rename"])

    def _check_sql_slave_status(self):
        expected_range = self.state["current_executed_gtid_target"]
        expected_index = self.state["current_relay_log_target"]

        with self._mysql_cursor() as cursor:
            cursor.execute("SHOW SLAVE STATUS")
            slave_status = cursor.fetchone()
            current_file = slave_status["Relay_Log_File"]
            sql_running_state = slave_status["Slave_SQL_Running_State"]
            current_index = int(current_file.rsplit(".", 1)[-1])
            if expected_index is not None and current_index < expected_index:
                self.log.debug("Expected relay log name not reached (%r < %r)", current_index, expected_index)
                if sql_running_state == "Slave has read all relay log; waiting for more updates":
                    # Sometimes if the next file is empty MySQL SQL thread does not update the relay log
                    # file to match the last one. Because the thread has finished doing anything we need
                    # to react to the situation or else restoration will stall indefinitely.
                    if expected_range:
                        self.log.info(
                            "SQL thread has finished executing even though target file has not been reached (%r < %r), "
                            "target GTID range has been set. Continuing with GTID check", current_index, expected_index
                        )
                    else:
                        # We don't quite know if proceeding is safe but there's no other sensible action than
                        # returning `True, expected_index` from this branch as we know there aren't any transactions
                        # that should be applied anyway so there should be no data loss.
                        self.log.warning(
                            "SQL thread has finished executing even though target file has not been reached (%r < %r), "
                            "no GTID range set. Considering complete", current_index, expected_index
                        )
                        return True, expected_index
                else:
                    return False, current_index
            # The batch we're applying might not have contained any GTIDs
            if not expected_range:
                self.log.info(
                    "No expected GTID range available, assuming complete because Relay_Log_File (%r) matches", current_file
                )
                found = True
            else:
                range_str = make_gtid_range_string([expected_range])
                cursor.execute(
                    "SELECT GTID_SUBSET(%s, @@GLOBAL.gtid_executed) AS executed, @@GLOBAL.gtid_executed AS gtid_executed",
                    [range_str]
                )
                result = cursor.fetchone()
                found = result["executed"]
                if found:
                    self.log.info(
                        "Expected log file %r reached and GTID range %r has been applied: %s", current_file, expected_range,
                        result["gtid_executed"]
                    )
                    # In some cases SQL thread doesn't change Relay_Log_File value appropriately. Update
                    # the index we return from here to match expected index if all transactions have been
                    # applied so that all applying binlogs are marked as completed even if the SQL thread
                    # did not say so.
                    if expected_index is not None and current_index < expected_index:
                        current_index = expected_index
            if found:
                cursor.execute("STOP SLAVE")
                # Current file could've been updated since we checked it the first time before slave was stopped.
                # Get the latest value here so that we're sure to start from correct index
                cursor.execute("SHOW SLAVE STATUS")
                current_file = cursor.fetchone()["Relay_Log_File"]
                last_index = int(current_file.rsplit(".", 1)[-1])
                if last_index > current_index:
                    self.log.info("Relay index incremented from %s to %s after STOP SLAVE", current_index, last_index)
                    current_index = last_index
            return found, current_index

    def _ensure_mysql_server_is_started(self, *, with_binlog, with_gtids):
        if self.state["mysql_params"] == {"with_binlog": with_binlog, "with_gtids": with_gtids}:
            return

        self.restart_mysqld_callback(with_binlog=with_binlog, with_gtids=with_gtids)
        server_uuid = self.state["server_uuid"]
        if not server_uuid:
            with self._mysql_cursor() as cursor:
                cursor.execute("SELECT @@GLOBAL.server_uuid AS server_uuid")
                server_uuid = cursor.fetchone()["server_uuid"]
        self.update_state(mysql_params={"with_binlog": with_binlog, "with_gtids": with_gtids}, server_uuid=server_uuid)

    def _patch_gtid_executed(self, binlog):
        if "patch_gtid_executed" in self.state["completed_apply_ops"]:
            return

        expected_gtid_executed_ranges = self._parse_gtid_executed_ranges(binlog)
        if not expected_gtid_executed_ranges:
            return
        self._ensure_mysql_server_is_started(with_binlog=False, with_gtids=False)

        with self._mysql_cursor() as cursor:
            for gtid_range in expected_gtid_executed_ranges:
                cursor.execute(
                    (
                        "SELECT interval_start, interval_end FROM mysql.gtid_executed "
                        "  WHERE (source_uuid, interval_start) IN ("
                        "    SELECT source_uuid, MAX(interval_start) FROM mysql.gtid_executed "
                        "      WHERE source_uuid = %s GROUP BY source_uuid"
                        "  )"
                    ),
                    [gtid_range["server_uuid"]],
                )
                existing_range = cursor.fetchone()
                if not existing_range:
                    # Range doesn't exist if there were no entries with GTID by the time basebackup creation
                    # completed
                    if gtid_range["start"] == 1:
                        cursor.execute((
                            "INSERT INTO mysql.gtid_executed (source_uuid, interval_start, interval_end) "
                            " VALUES (%s, %s, %s)"
                        ), (gtid_range["server_uuid"], gtid_range["start"], gtid_range["end"]))
                        cursor.execute("COMMIT")
                    else:
                        # This is not expected to happen. We cannot ensure gtid_executed is in sane state if it
                        # happens but applying old binlog is not dependent on this so allow continuing regardless
                        self.log.error("Could not find existing gtid_executed info for range %r", gtid_range)
                    continue

                if existing_range["interval_end"] == gtid_range["end"]:
                    self.log.info("Existing gtid_executed info already up-to-date, no need to apply %r", gtid_range)
                    continue

                if existing_range["interval_end"] != gtid_range["start"] - 1:
                    # This usually shouldn't happen because gtid_executed is updated whenever binlog is
                    # rotated so all missing values should've been found from the binlog we parsed. There
                    # seem to be some corner cases where the backup still ends up containing older GTID
                    # executed value so that there's a gap in the sequence.
                    self.log.info(
                        "Existing gtid_executed %r does not end just before new range %r", existing_range, gtid_range
                    )

                cursor.execute(
                    ("UPDATE mysql.gtid_executed SET interval_end = %s "
                     "  WHERE source_uuid = %s AND interval_start = %s"),
                    (gtid_range["end"], gtid_range["server_uuid"], existing_range["interval_start"]),
                )
                cursor.execute("COMMIT")

        self.state.update(completed_apply_ops=self.state["completed_apply_ops"] + ["patch_gtid_executed"])

    def _relay_log_name(self, *, index, full_path=True):
        return relay_log_name(prefix=self.mysql_relay_log_prefix, index=index, full_path=full_path)

    def _relay_log_prefetch_name(self, *, index):
        local_name = self._relay_log_name(index=index)
        return f"{local_name}.prefetch"

    def _start_process_pool(self):
        process_count = max(multiprocessing.cpu_count() - 1, 1)
        config = {
            "object_storage": self.file_storage_config,
            "rsa_private_key_pem": self.rsa_private_key_pem.decode("ascii"),
        }
        self.worker_processes = [
            self.mp_context.Process(target=download_binlog, args=(config, self.queue_out, self.queue_in))
            for _ in range(process_count)
        ]
        for worker in self.worker_processes:
            worker.start()

    def _switch_to_next_binlog_stream(self):
        current_index = self.state["current_binlog_stream_index"]
        if current_index + 1 >= len(self.binlog_streams):
            return False

        # _switch_to_next_binlog_stream is only ever called when we have exactly one remaining binlog,
        # which is the last one we'll process for the previous stream. The adjusted remote index for that
        # is the number we'll want to add to the indexes for next stream as that has its indexes start
        # from one.
        binlog_stream_offset = self.pending_binlogs[-1]["adjusted_remote_index"]
        self.update_state(binlog_stream_offset=binlog_stream_offset, current_binlog_stream_index=current_index + 1)
        self.log.info(
            "Switched to binlog stream index %s, index adjustment set to %s", current_index + 1, binlog_stream_offset
        )
        return True
