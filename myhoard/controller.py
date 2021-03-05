# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import contextlib
import datetime
import enum
import json
import logging
import math
import os
import threading
import time

import pymysql
from pghoard.rohmu import get_transfer
from pghoard.rohmu.compressor import DecompressSink
from pghoard.rohmu.encryptor import DecryptSink

from .backup_stream import BackupStream
from .binlog_scanner import BinlogScanner
from .errors import BadRequest
from .restore_coordinator import RestoreCoordinator
from .state_manager import StateManager
from .util import (
    are_gtids_in_executed_set,
    change_master_to,
    DEFAULT_MYSQL_TIMEOUT,
    ERR_TIMEOUT,
    make_gtid_range_string,
    mysql_cursor,
    parse_fs_metadata,
    relay_log_name,
)

ERR_CANNOT_CONNECT = 2003


class Controller(threading.Thread):
    """Main logic controller for the service. This drives the individual handlers like
    BackupStream, BinlogScanner and RestoreCoordinator as well as provides state info
    that can be made available by other components like an HTTP server."""

    @enum.unique
    class Mode(str, enum.Enum):
        # Actively back up the system and perform related auxiliary operations
        active = "active"
        # Waiting for command to determine which state to enter
        idle = "idle"
        # Observe backup progress by other nodes and perform related auxiliary operations
        observe = "observe"
        # Promote current server into active one
        promote = "promote"
        # Restore system to given backup
        restore = "restore"

    BACKUP_REFRESH_INTERVAL_BASE = 120
    # We don't expect anyone but the single active MyHoard to make any changes to backups but we still want
    # to sometimes check there aren't some unexpected changes. The "sometimes" can be pretty infrequently
    BACKUP_REFRESH_ACTIVE_MULTIPLIER = 10
    ITERATION_SLEEP = 1

    def __init__(
        self,
        *,
        backup_settings,
        backup_sites,
        binlog_purge_settings,
        mysql_binlog_prefix,
        mysql_client_params,
        mysql_config_file_name,
        mysql_data_directory,
        mysql_relay_log_index_file,
        mysql_relay_log_prefix,
        restart_mysqld_callback,
        restore_max_binlog_bytes,
        server_id,
        state_dir,
        stats,
        temp_dir,
    ):
        super().__init__()
        self.backup_refresh_interval_base = self.BACKUP_REFRESH_INTERVAL_BASE
        self.backup_settings = backup_settings
        self.backup_sites = backup_sites
        self.backup_streams = []
        self.backup_streams_initialized = False
        self.binlog_not_caught_log_counter = 0
        self.binlog_purge_settings = binlog_purge_settings
        scanner_state_file = os.path.join(state_dir, "binlog_scanner_state.json")
        self.binlog_scanner = BinlogScanner(
            binlog_prefix=mysql_binlog_prefix,
            server_id=server_id,
            state_file=scanner_state_file,
            stats=stats,
        )
        self.is_running = True
        self.iteration_sleep = self.ITERATION_SLEEP
        self.lock = threading.RLock()
        self.log = logging.getLogger(self.__class__.__name__)
        self.max_binlog_bytes = None
        self.mysql_client_params = mysql_client_params
        self.mysql_config_file_name = mysql_config_file_name
        self.mysql_data_directory = mysql_data_directory
        self.mysql_relay_log_index_file = mysql_relay_log_index_file
        self.mysql_relay_log_prefix = mysql_relay_log_prefix
        self.restart_mysqld_callback = restart_mysqld_callback
        self.restore_max_binlog_bytes = restore_max_binlog_bytes
        self.restore_coordinator = None
        self.seen_basebackup_infos = {}
        self.server_id = server_id
        self.site_transfers = {}
        self.state = {
            "backup_request": {},
            "backups": [],
            "backups_fetched_at": 0,
            "binlogs_purged_at": 0,
            "errors": 0,
            "force_promote": False,
            "last_binlog_purge": time.time(),
            "last_binlog_rotation": time.time(),
            "last_could_have_purged": time.time(),
            "mode": self.Mode.idle,
            "owned_stream_ids": [],
            "promote_details": {},
            "promote_on_restore_completion": False,
            "replication_state": {},
            "restore_options": {},
            "server_uuid": None,
            "uploaded_binlogs": [],
        }
        self.state_dir = state_dir
        state_file = os.path.join(state_dir, "myhoard_controller_state.json")
        self.state_manager = StateManager(lock=self.lock, state=self.state, state_file=state_file)
        self.stats = stats
        self.temp_dir = temp_dir
        self.wakeup_event = threading.Event()
        self._get_upload_backup_site()
        self._update_mode_tag()

    def is_log_backed_up(self, *, log_index):
        return all(
            # Only consider streams that are actively backing up binlogs
            not backup_stream.is_streaming_binlogs() or backup_stream.is_log_backed_up(log_index=log_index)
            for backup_stream in self.backup_streams
        )

    def is_safe_to_reload(self):
        restore_coordinator = self.restore_coordinator
        if restore_coordinator and restore_coordinator.phase == RestoreCoordinator.Phase.restoring_basebackup:
            return False
        with self.lock:
            for stream in self.backup_streams:
                if stream.active_phase == BackupStream.ActivePhase.basebackup:
                    return False
        return True

    def mark_backup_requested(self, *, backup_reason, normalized_backup_time=None):
        normalized_backup_time = normalized_backup_time or self._current_normalized_backup_timestamp()
        new_request = {"backup_reason": backup_reason, "normalized_backup_time": normalized_backup_time}
        with self.lock:
            old = self.state["backup_request"]
            if old:
                if (
                    old == new_request or normalized_backup_time < old["normalized_backup_time"]
                    # Prefer storing "scheduled" as backup reason since that reduces chance of trying to correct
                    # backup schedule too quickly in case of backup time has been changed and manual backup is created
                    or (
                        normalized_backup_time == old["normalized_backup_time"]
                        and old["backup_reason"] == BackupStream.BackupReason.scheduled
                    )
                ):
                    return
            self.state_manager.update_state(backup_request=new_request)

    @property
    def mode(self):
        return self.state["mode"]

    def restore_backup(self, *, site, stream_id, target_time=None, target_time_approximate_ok=None):
        with self.lock:
            if self.mode != self.Mode.idle:
                # Could consider allowing restore request also when mode is `restore`
                raise ValueError("Current mode is {}, restore only allowed while in idle mode".format(self.mode))

            for backup in list(self.state["backups"]):
                if backup["stream_id"] != stream_id or backup["site"] != site:
                    continue
                if not backup["basebackup_info"]:
                    raise ValueError("Backup {!r} cannot be restored".format(backup))
                if target_time:
                    if target_time < backup["basebackup_info"]["end_ts"]:
                        raise ValueError(
                            "Requested target time {} predates backup completion: {!r}".format(target_time, backup)
                        )
                    # Caller must make sure they pick a backup that contains the requested target time. If this backup
                    # has been closed (will not get any further updates) at a time that is before the requested target
                    # time it is not possible to satisfy the request
                    if backup["closed_at"] and target_time > backup["closed_at"]:
                        raise ValueError("Requested target time {} is after backup close: {!r}".format(target_time, backup))
                break
            else:
                raise ValueError(f"Requested backup {stream_id!r} for site {site!r} not found")

            self.log.info(
                "Restoring backup stream %r, target time %r%s", stream_id, target_time,
                " (approximate time)" if target_time_approximate_ok else ""
            )
            self.state_manager.update_state(
                mode=self.Mode.restore,
                restore_options={
                    "binlog_streams": [{
                        "site": site,
                        "stream_id": stream_id,
                    }],
                    "pending_binlogs_state_file": self._get_restore_coordinator_pending_state_file_and_remove_old(),
                    "state_file": self._get_restore_coordinator_state_file_and_remove_old(),
                    "stream_id": stream_id,
                    "site": site,
                    "target_time": target_time,
                    "target_time_approximate_ok": target_time_approximate_ok,
                }
            )
            self._update_mode_tag()

        self.wakeup_event.set()

    def rotate_and_back_up_binlog(self):
        local_log_index = self._rotate_binlog()
        self.wakeup_event.set()
        return local_log_index

    def run(self):
        self.log.info("Controller running")
        while self.is_running:
            try:
                if self.mode == self.Mode.idle:
                    self._handle_mode_idle()
                elif self.mode == self.Mode.restore:
                    self._handle_mode_restore()
                elif self.mode == self.Mode.active:
                    self._handle_mode_active()
                elif self.mode == self.Mode.observe:
                    self._handle_mode_observe()
                elif self.mode == self.Mode.promote:
                    self._handle_mode_promote()
                else:
                    assert False, "Invalid mode {}".format(self.mode)
                self.wakeup_event.wait(self._get_iteration_sleep())
                self.wakeup_event.clear()
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unexpected exception in mode %s", self.mode)
                self.stats.unexpected_exception(ex=ex, where="Controller.run")
                self.state_manager.increment_counter(name="errors")
                self.stats.increase("myhoard.generic_errors")
                time.sleep(self.iteration_sleep)
        self.is_running = False

    def stop(self):
        self.log.info("Stopping controller")
        self.is_running = False
        self.wakeup_event.set()
        with contextlib.suppress(Exception):
            self.join()
        if self.restore_coordinator:
            self.restore_coordinator.stop()
        for stream in self.backup_streams:
            stream.stop()
        self.log.info("Controller stopped")

    def switch_to_active_mode(self, *, force=False):
        """Requests switching from idle, observe or restore mode to active mode. This does
        not immediately switch mode to active but instead switches to promote mode first, which
        automatically switches to active mode once promotion flow has been successfully completed"""
        with self.lock:
            # If current mode is promote and some binlogs are being applied, set a flag indicating that
            # promotion should be considered complete even if applying the binary logs has not completed
            if self.mode == self.Mode.promote and force and self.state["promote_details"].get("binlogs_applying"):
                self.state_manager.update_state(force_promote=True)
                return
            elif self.mode == self.Mode.restore:
                if not force:
                    self._fail_if_restore_is_not_complete()
                else:
                    if not self.restore_coordinator:
                        raise ValueError("Cannot switch mode, current restoration state is indeterminate")
                    self.restore_coordinator.force_completion()
                    self.state_manager.update_state(force_promote=True, promote_on_restore_completion=True)
                    return
            elif force:
                raise BadRequest("Can only force promotion while waiting for binlogs to be applied")
            elif self.mode in {self.Mode.active, self.Mode.promote}:
                self.log.info("Already in %s mode when switch to active mode was requested", self.mode)
                return
            elif self.mode == self.Mode.observe:
                self._fail_if_observe_to_active_switch_is_not_allowed()
            self.state_manager.update_state(
                # Ensure latest backup list is fetched before promotion so that we
                # start working with appropriate backup streams
                backups_fetched_at=0,
                mode=self.Mode.promote,
                restore_options={},
            )
            self._update_mode_tag()
        self.wakeup_event.set()

    def switch_to_observe_mode(self):
        """Request switching from idle or restore mode to observe mode"""
        with self.lock:
            if self.mode == self.Mode.observe:
                self.log.info("Requested switch to observe mode but currently mode is already that")
                return
            elif self.mode in {self.Mode.active, self.Mode.promote}:
                # Master (or almost master) cannot become a standby
                raise ValueError(f"Switch from {self.mode} to observe mode is not allowed")
            elif self.mode == self.Mode.restore:
                self._fail_if_restore_is_not_complete()
                self._fail_if_not_read_only()
            self.state_manager.update_state(
                backups_fetched_at=0,
                mode=self.Mode.observe,
                restore_options={},
            )
            self._update_mode_tag()

    @classmethod
    def collect_binlogs_to_purge(
        cls, *, backup_streams, binlogs, exclude_uuid=None, log, mode, purge_settings, replication_state
    ):
        only_binlogs_without_gtids = None
        binlogs_to_purge = []
        binlogs_to_maybe_purge = []
        for binlog in binlogs:
            binlog_age = time.time() - binlog["processed_at"]
            min_age = purge_settings["min_binlog_age_before_purge"]
            if binlog_age < min_age:
                log.info(
                    "Binlog %s was processed %s seconds ago and min age before purging is %s seconds, not purging",
                    binlog["local_index"], math.ceil(binlog_age), min_age
                )
                break
            if mode == cls.Mode.active:
                # In active mode we want all streams to say purging a binlog is safe
                can_purge = all(
                    stream.is_binlog_safe_to_delete(binlog, exclude_uuid=exclude_uuid) for stream in backup_streams
                )
                if not can_purge:
                    log.info("Binlog %s reported not safe to delete by some backup streams", binlog["local_index"])
            elif purge_settings["purge_when_observe_no_streams"] and not backup_streams:
                log.info("No backup streams and purging is allowed, assuming purging %s is safe", binlog["local_index"])
                can_purge = True
            else:
                # Any stream that has basebackup info (is resumable) must say purging is safe and there must
                # be at least one such stream. For other streams we don't care.
                at_least_one_safe_stream = False
                at_least_one_unsafe_stream = False
                for stream in backup_streams:
                    if stream.state["basebackup_info"]:
                        if stream.is_binlog_safe_to_delete(  # pylint: disable=simplifiable-if-statement
                            binlog, exclude_uuid=exclude_uuid
                        ):
                            at_least_one_safe_stream = True
                        else:
                            at_least_one_unsafe_stream = True
                can_purge = at_least_one_safe_stream and not at_least_one_unsafe_stream
                if can_purge:
                    log.info(
                        "Binlog %s is reported safe to delete by at least one stream and not as unsafe by any",
                        binlog["local_index"]
                    )
                else:
                    log.info(
                        "Binlog %s either reported as unsafe to delete (%s) by some stream or not reported as safe to "
                        "delete by any (%s)", binlog["local_index"], at_least_one_unsafe_stream, at_least_one_safe_stream
                    )
            if not can_purge:
                break
            # If we haven't been informed of any replication state assume purging is safe for any backed up binlog
            if not replication_state:
                log.info("No replication state set, assuming purging binlog %s is safe", binlog["local_index"])
                binlogs_to_purge.append(binlog)
            elif not binlog["gtid_ranges"]:
                if only_binlogs_without_gtids is None:
                    only_binlogs_without_gtids = True
                if mode == cls.Mode.observe:
                    binlogs_to_purge.append(binlog)
                else:
                    # Maybe purge this. We cannot tell based on the information we have whether deleting is safe because
                    # we only have replication GTID info available but not info about which files each server has
                    # replicated. If we delete a file that is after any server's current position, even if the file is
                    # empty, replication will break. We know this is safe to purge when we encounter at least one binlog
                    # with GTIDs after this one and those GTIDs have all been replicated.
                    binlogs_to_maybe_purge.append(binlog)
            else:
                only_binlogs_without_gtids = False
                for server_name, gtid_executed in replication_state.items():
                    if not are_gtids_in_executed_set(gtid_executed, binlog["gtid_ranges"], exclude_uuid=exclude_uuid):
                        log.info(
                            "Binlog %s not yet replicated to server %r, not purging", binlog["local_index"], server_name
                        )
                        can_purge = False
                        break
                if can_purge:
                    log.info("Binlog %s has been replicated to all servers, purging", binlog["local_index"])
                    binlogs_to_purge.extend(binlogs_to_maybe_purge)
                    binlogs_to_maybe_purge = []
                    binlogs_to_purge.append(binlog)
                else:
                    break
        return binlogs_to_purge, only_binlogs_without_gtids

    @staticmethod
    def get_backup_list(backup_sites, *, seen_basebackup_infos=None, site_transfers=None):
        if seen_basebackup_infos is None:
            seen_basebackup_infos = {}
        if site_transfers is None:
            site_transfers = {}
        backups = []
        for site_name, site_config in backup_sites.items():
            file_storage = site_transfers.get(site_name)
            if file_storage is None:
                file_storage = get_transfer(site_config["object_storage"])
                site_transfers[site_name] = file_storage
            streams = list(file_storage.list_prefixes(site_name))
            for site_and_stream_id in streams:
                basebackup_compressed_size = None
                basebackup_info = None
                closed_info = None
                completed_info = None
                for info in file_storage.list_iter(site_and_stream_id):
                    file_name = info["name"].rsplit("/", 1)[-1]
                    if file_name == "basebackup.xbstream":
                        basebackup_compressed_size = info["size"]
                    elif file_name == "basebackup.json":
                        # The basebackup info json contents never change after creation so we can use cached
                        # value if available to avoid re-fetching the same content over and over again
                        basebackup_info = seen_basebackup_infos.get(site_and_stream_id)
                        if basebackup_info is None:
                            info_str, _ = file_storage.get_contents_to_string(info["name"])
                            basebackup_info = json.loads(info_str.decode("utf-8"))
                            seen_basebackup_infos[site_and_stream_id] = basebackup_info
                    elif file_name == "closed.json":
                        closed_info = parse_fs_metadata(info["metadata"])
                    elif file_name == "completed.json":
                        completed_info = parse_fs_metadata(info["metadata"])

                if basebackup_info and basebackup_compressed_size:
                    basebackup_info = dict(basebackup_info, compressed_size=basebackup_compressed_size)
                resumable = basebackup_info and basebackup_compressed_size
                completed = resumable and completed_info
                closed = completed and closed_info
                backups.append({
                    "basebackup_info": basebackup_info,
                    "closed_at": closed_info["closed_at"] if closed else None,
                    "completed_at": completed_info["completed_at"] if completed else None,
                    "recovery_site": site_config.get("recovery_only", False),
                    "stream_id": site_and_stream_id.rsplit("/", 1)[-1],
                    "resumable": bool(resumable),
                    "site": site_name,
                })
        return backups

    def _apply_downloaded_remote_binlogs(self):
        to_apply = self.state["promote_details"].get("binlogs_to_apply")
        if self.state["promote_details"].get("binlogs_applying") or not to_apply:
            return

        expected_ranges = []
        with mysql_cursor(**self.mysql_client_params) as cursor:
            # Stop IO and SQL slaves so that we can flush relay logs and retain the old log files. This allows
            # us to replace the empty files with ones that have actual content and make the SQL thread apply
            # them. Same as with regular restoration.
            cursor.execute("STOP SLAVE")
            # Get current slave status so that we know which relay logs to reuse
            cursor.execute("SHOW SLAVE STATUS")
            slave_status = cursor.fetchone()
            first_name = slave_status["Relay_Log_File"]
            if not first_name:
                first_name = "relay.000001"
            if not self.state["promote_details"].get("relay_index_updated"):
                first_index = int(first_name.split(".")[-1])
                if (
                    first_index == 1 and not slave_status["Relay_Master_Log_File"]
                    and not slave_status["Exec_Master_Log_Pos"] and not slave_status["Retrieved_Gtid_Set"]
                ):
                    # FLUSH RELAY LOGS does nothing if RESET SLAVE has been called since last call to CHANGE MASTER TO
                    self.log.info(
                        "Slave status is empty, assuming RESET SLAVE has been executed and writing relay index manually"
                    )
                    with open(self.mysql_relay_log_index_file, "wb") as index_file:
                        names = [self._relay_log_name(index=i + 1, full_path=False) for i in range(len(to_apply))]
                        index_file.write(("\n".join(names) + "\n").encode("utf-8"))
                    self.log.info("Wrote names: %s", names)
                else:
                    for _ in to_apply:
                        cursor.execute("FLUSH RELAY LOGS")
                self.state_manager.update_state(
                    promote_details={
                        **self.state["promote_details"],
                        "relay_index_updated": True,
                    }
                )
            for idx, binlog in enumerate(to_apply):
                if not self.state["promote_details"].get("relay_logs_renamed"):
                    os.rename(binlog["local_prefetch_name"], self._relay_log_name(index=first_index + idx))
                    self.log.info(
                        "Renamed %r to %r", binlog["local_prefetch_name"], self._relay_log_name(index=first_index + idx)
                    )
                expected_ranges.extend(binlog["gtid_ranges"])
            if not self.state["promote_details"].get("relay_logs_renamed"):
                self.state_manager.update_state(
                    promote_details={
                        **self.state["promote_details"],
                        "relay_logs_renamed": True,
                    }
                )
            # Make SQL thread replay relay logs starting from where we have replaced empty / old logs with
            # new ones that have actual valid binlogs from previous master
            options = {
                "MASTER_AUTO_POSITION": 0,
                "MASTER_HOST": "dummy",
                "RELAY_LOG_FILE": first_name,
                "RELAY_LOG_POS": 4,
            }
            change_master_to(cursor=cursor, options=options)
            cursor.execute("START SLAVE SQL_THREAD")
            expected_file = self._relay_log_name(index=first_index + len(to_apply), full_path=False)
            expected_ranges = make_gtid_range_string(expected_ranges)
            self.log.info(
                "Started SQL thread, waiting for file %r and GTID range %r to be reached", expected_file, expected_ranges
            )
            self.state_manager.update_state(
                promote_details={
                    **self.state["promote_details"],
                    "binlogs_applying": to_apply,
                    "binlogs_to_apply": [],
                    "expected_file": expected_file,
                    "expected_ranges": expected_ranges,
                }
            )

    def _binlog_uploaded(self, *, local_index, remote_key, stream):
        # Do the actual update calls from main controller thread to avoid any other binlog updates
        # happening to be ongoing at the same time that could cause hard to debug random failures
        with self.lock:
            binlog_info = {
                "exclude_stream_id": stream.stream_id,
                "local_index": local_index,
                "remote_key": remote_key,
            }
            self.state_manager.update_state(uploaded_binlogs=self.state["uploaded_binlogs"] + [binlog_info])

    def _build_backup_stream(self, backup):
        stream_id = backup["stream_id"]
        backup_site = self.backup_sites[backup["site"]]
        # Some of the values being passed here like backup_reason will be set correctly either based on
        # data stored in local state file if available or in backup file storage if local state is not available
        return BackupStream(
            backup_reason=None,
            compression=backup_site.get("compression"),
            file_storage_setup_fn=lambda: get_transfer(backup_site["object_storage"]),
            file_uploaded_callback=self._binlog_uploaded,
            # Always create in observe mode, will be switched to
            # active mode later if needed
            mode=BackupStream.Mode.observe,
            mysql_client_params=self.mysql_client_params,
            mysql_config_file_name=self.mysql_config_file_name,
            mysql_data_directory=self.mysql_data_directory,
            normalized_backup_time=None,
            rsa_public_key_pem=backup_site["encryption_keys"]["public"],
            server_id=self.server_id,
            state_file=self._state_file_from_stream_id(stream_id),
            site=backup["site"],
            stats=self.stats,
            stream_id=stream_id,
            temp_dir=self.temp_dir,
        )

    def _cache_server_uuid_if_missing(self):
        if self.state["server_uuid"]:
            return

        with mysql_cursor(**self.mysql_client_params) as cursor:
            cursor.execute("SELECT @@GLOBAL.server_uuid AS server_uuid")
            server_uuid = cursor.fetchone()["server_uuid"]
        self.state_manager.update_state(server_uuid=server_uuid)

    def _check_binlog_apply_status(self):
        binlogs = self.state["promote_details"].get("binlogs_applying")
        if not binlogs:
            return

        expected_file = self.state["promote_details"].get("expected_file")
        expected_ranges = self.state["promote_details"].get("expected_ranges")

        with mysql_cursor(**self.mysql_client_params) as cursor:
            cursor.execute("SHOW SLAVE STATUS")
            slave_status = cursor.fetchone()
            current_file = slave_status["Relay_Log_File"]
            reached_target = True
            if current_file != expected_file:
                reached_target = False
            elif expected_ranges:
                cursor.execute("SELECT GTID_SUBSET(%s, @@GLOBAL.gtid_executed) AS executed", [expected_ranges])
                if not cursor.fetchone()["executed"]:
                    reached_target = False
            if not reached_target:
                if self.state["force_promote"]:
                    self.log.warning("Promotion target state not reached but forced promotion requested")
                else:
                    sql_thread_running = slave_status["Slave_SQL_Running"]
                    if sql_thread_running != "Yes":
                        self.log.warning("Expected SQL thread to be running state is %s, starting it", sql_thread_running)
                        cursor.execute("START SLAVE SQL_THREAD")
                    return
            else:
                self.log.info("Expected relay log (%r) and GTIDs reached (%r)", expected_file, expected_ranges)
            cursor.execute("STOP SLAVE")
            promote_details = {
                **self.state["promote_details"],
                "binlogs_applying": [],
                "expected_file": None,
                "expected_ranges": None,
            }
            if not reached_target and self.state["force_promote"]:
                promote_details["binlogs_to_apply"] = []
                promote_details["binlogs_to_fetch"] = []
            self.state_manager.update_state(promote_details=promote_details)

    def _create_new_backup_stream_if_requested_and_max_streams_not_exceeded(self):
        # Only ever have two open backup streams. Uploading binlogs to more streams than that is
        # unlikely to improve the system behavior. We'll create new backup stream once the latter
        # one catches up with the first, the first is marked as closed, and removed from our list.
        if len(self.backup_streams) >= 2:
            return
        with self.lock:
            request = self.state["backup_request"]
            if request:
                self._start_new_backup(
                    backup_reason=request["backup_reason"],
                    normalized_backup_time=request["normalized_backup_time"],
                )

    def _create_restore_coordinator_if_missing(self):
        if self.restore_coordinator:
            return

        options = self.state["restore_options"]
        backup_site = self.backup_sites[options["site"]]
        storage_config = backup_site["object_storage"]
        self.log.info("Creating new restore coordinator")
        self.restore_coordinator = RestoreCoordinator(
            binlog_streams=options["binlog_streams"],
            file_storage_config=storage_config,
            max_binlog_bytes=self.restore_max_binlog_bytes,
            mysql_client_params=self.mysql_client_params,
            mysql_config_file_name=self.mysql_config_file_name,
            mysql_data_directory=self.mysql_data_directory,
            mysql_relay_log_index_file=self.mysql_relay_log_index_file,
            mysql_relay_log_prefix=self.mysql_relay_log_prefix,
            pending_binlogs_state_file=options["pending_binlogs_state_file"],
            restart_mysqld_callback=self.restart_mysqld_callback,
            rsa_private_key_pem=backup_site["encryption_keys"]["private"],
            site=options["site"],
            state_file=options["state_file"],
            stats=self.stats,
            stream_id=options["stream_id"],
            target_time=options["target_time"],
            target_time_approximate_ok=options["target_time_approximate_ok"],
            temp_dir=self.temp_dir,
        )
        if not self.restore_coordinator.is_complete():
            self.log.info("Starting restore coordinator")
            self.restore_coordinator.start()
        else:
            self.log.info("Newly created restore coordinator is already in completed state")

    def _current_normalized_backup_timestamp(self):
        """Returns the closest historical backup time that current time matches to (or current time if it matches).
        E.g. if backup hour is 13, backup minute is 50, current time is 15:40 and backup interval is 60 minutes,
        the return value is 14:50 today. If backup hour and minute are as before, backup interval is 1440 and
        current time is 13:45 the return value is 13:50 yesterday."""
        now = datetime.datetime.now(datetime.timezone.utc)
        normalized = now
        backup_interval_minutes = self.backup_settings["backup_interval_minutes"]
        backup_hour = self.backup_settings["backup_hour"]
        backup_minute = self.backup_settings["backup_minute"]
        if normalized.hour < backup_hour or (normalized.hour == backup_hour and normalized.minute < backup_minute):
            normalized = normalized - datetime.timedelta(days=1)
        normalized = normalized.replace(hour=backup_hour, minute=backup_minute, second=0, microsecond=0)
        while normalized + datetime.timedelta(minutes=backup_interval_minutes) < now:
            normalized = normalized + datetime.timedelta(minutes=backup_interval_minutes)
        return normalized.isoformat()

    def _determine_unapplied_remote_binlogs(self, stream):
        """Finds out if given stream contains any remote binlogs that have GTIDs that have not
        yet been applied locally. Possibly found binlogs are stored in state so that they get
        downloaded and applied."""
        missing_checked_key = f"{stream.stream_id}.missing_checked"
        if self.state["promote_details"].get(missing_checked_key) or self.state["force_promote"]:
            return

        already_processed_remote_indexes = set()
        for key in {"binlogs_to_fetch", "binlogs_to_apply", "binlogs_applying"}:
            for binlog in self.state["promote_details"].get(key, []):
                already_processed_remote_indexes.add(binlog["remote_index"])

        missing_binlogs = []
        missing_gtids = False
        with mysql_cursor(**self.mysql_client_params) as cursor:
            for binlog in stream.iterate_remote_binlogs(reverse=True):
                if binlog["remote_index"] in already_processed_remote_indexes:
                    break
                if not binlog["gtid_ranges"]:
                    missing_binlogs.insert(0, binlog)
                else:
                    gtid_str = make_gtid_range_string(binlog["gtid_ranges"])
                    cursor.execute("SELECT GTID_SUBSET(%s, @@GLOBAL.gtid_executed) AS executed", [gtid_str])
                    executed = cursor.fetchone()["executed"]
                    if executed:
                        break
                    missing_binlogs.insert(0, binlog)
                    missing_gtids = True

        binlogs_to_fetch = self.state["promote_details"].get("binlogs_to_fetch", [])
        # No point in applying binlogs that don't have any GTIDs in them (we shouldn't even have such binlogs)
        # as we don't know whether those really are applied or not and applying them doesn't change things anyway
        if missing_gtids:
            # New binlogs must be after the earlier binlogs because we skipped any that had already been seen
            site = self._get_site_for_stream_id(stream.stream_id)
            missing_binlogs = [{**binlog, "site": site} for binlog in missing_binlogs]
            binlogs_to_fetch = binlogs_to_fetch + missing_binlogs

        self.state_manager.update_state(
            promote_details={
                **self.state["promote_details"],
                missing_checked_key: True,
                "binlogs_to_fetch": binlogs_to_fetch,
            }
        )

    def _download_unapplied_remote_binlogs(self):
        """Download any binlogs that master has uploaded to file storage but we haven't applied.
        In normal situation there shouldn't be any and in abnormal situation there should only be
        one or two so don't bother with any multiprocess complexity."""
        # Make a copy in case the array gets modified
        binlogs_to_fetch = self.state["promote_details"].get("binlogs_to_fetch", [])[:]
        for binlog in binlogs_to_fetch:
            remote_index = binlog["remote_index"]
            # Prefetch name doesn't matter, we'll anyway use whatever indexes the server
            # is currently using
            local_name = self._relay_log_name(index=remote_index)
            prefetch_name = f"{local_name}.prefetch"
            binlog["local_prefetch_name"] = prefetch_name
            start_time = time.monotonic()
            with contextlib.suppress(OSError):
                os.remove(prefetch_name)
            with open(prefetch_name, "wb") as output_file:
                output_obj = DecompressSink(output_file, binlog["compression_algorithm"])
                site = binlog["site"]
                backup_site = self.backup_sites[site]
                output_obj = DecryptSink(output_obj, binlog["remote_file_size"], backup_site["encryption_keys"]["private"])
                transfer = self.site_transfers.get(site)
                if transfer is None:
                    transfer = get_transfer(backup_site["object_storage"])
                    self.site_transfers[site] = transfer
                transfer.get_contents_to_fileobj(binlog["remote_key"], output_obj)
                self.log.info(
                    "%r successfully saved as %r in %.2f seconds", binlog["remote_key"], prefetch_name,
                    time.monotonic() - start_time
                )

            # Try to keep objects in state mostly immutable to avoid weird issues due to changes from different
            # threads when other one has a reference to the same object
            binlogs_to_fetch = binlogs_to_fetch[1:]
            binlogs_to_apply = self.state["promote_details"].get("binlogs_to_apply", []) + [binlog]
            self.state_manager.update_state(
                promote_details={
                    **self.state["promote_details"],
                    "binlogs_to_apply": binlogs_to_apply,
                    "binlogs_to_fetch": binlogs_to_fetch,
                },
            )

    def _extend_binlog_stream_list(self):
        """If we're currently restoring a backup to most recent point in time, checks for new available
        backup streams and if there is one adds that to the list of streams from which to apply binlogs.
        The reasoning for this logic is that if restoring binary logs takes a long time the current master
        could fail while we're restoring data but before failing it could've created new backup stream and
        uploaded some files there but not in the backup we're restoring, causing data loss when this node
        gets promoted after backup restoration completes and there's no available master."""
        if not self.restore_coordinator.can_add_binlog_streams():
            return
        backups = sorted(
            (backup for backup in self.state["backups"] if backup["completed_at"]), key=lambda backup: backup["completed_at"]
        )
        # If most recent current backup is not in the list of backups being restored then we're probably
        # restoring some old backup and don't want to automatically get latest changes
        if not any(bs["stream_id"] == backups[-1]["stream_id"] for bs in self.restore_coordinator.binlog_streams):
            return

        old_backups = [{"site": backup["site"], "stream_id": backup["stream_id"]} for backup in backups]
        self._refresh_backups_list()
        backups = sorted(
            (backup for backup in self.state["backups"] if backup["completed_at"]), key=lambda backup: backup["completed_at"]
        )
        new_backups = [{"site": backup["site"], "stream_id": backup["stream_id"]} for backup in backups]
        if old_backups == new_backups:
            return

        active_stream_found = False
        new_binlog_streams = []
        for backup in new_backups:
            if backup["stream_id"] == self.restore_coordinator.stream_id:
                active_stream_found = True
            elif active_stream_found:
                if backup not in self.restore_coordinator.binlog_streams:
                    new_binlog_streams.append(backup)

        if new_binlog_streams:
            if self.restore_coordinator.add_new_binlog_streams(new_binlog_streams):
                options = self.state["restore_options"]
                options = dict(options, binlog_streams=options["binlog_streams"] + new_binlog_streams)
                self.state_manager.update_state(restore_options=options)
                self.log.info("Added new binlog streams %r", new_binlog_streams)

    def _fail_if_not_read_only(self):
        with mysql_cursor(**self.mysql_client_params) as cursor:
            cursor.execute("SELECT @@GLOBAL.read_only AS read_only")
            if not cursor.fetchone()["read_only"]:
                raise Exception("System expected to be in read-only mode but isn't")

    def _fail_if_observe_to_active_switch_is_not_allowed(self):
        """Verifies that the MySQL server is in read-only mode with IO and SQL threads stopped"""
        with mysql_cursor(**self.mysql_client_params) as cursor:
            cursor.execute("SELECT @@GLOBAL.read_only AS read_only")
            if not cursor.fetchone()["read_only"]:
                raise Exception("System expected to be in read-only mode but isn't")
            cursor.execute("SHOW SLAVE STATUS")
            info = cursor.fetchone()
            if info is None:
                # None happens if RESET SLAVE has been performed or if the slave never was running, e.g.
                # because there were no binary logs to restore.
                self.log.warning("SHOW SLAVE STATUS returned no results.")
                return
            if info["Slave_IO_Running"] == "Yes":
                raise Exception("Slave IO thread expected to be stopped but is running")
            if info["Slave_SQL_Running"] == "Yes":
                if info["Slave_SQL_Running_State"] != "Slave has read all relay log; waiting for more updates":
                    raise Exception("Expected SQL thread to be stopped or finished processing updates")
                cursor.execute("STOP SLAVE SQL_THREAD")

    def _fail_if_restore_is_not_complete(self):
        if not self.restore_coordinator:
            # Edge case, shouldn't happen
            raise ValueError("Cannot switch mode, current restoration state is indeterminate")
        if not self.restore_coordinator.is_complete():
            raise ValueError("Cannot switch mode, ongoing restoration is not complete")

    def _get_iteration_sleep(self):
        # Sleep less when in promote mode because this should complete as soon as
        # possible to reduce downtime
        if self.mode == self.Mode.promote:
            return self.iteration_sleep / 10.0
        else:
            return self.iteration_sleep

    def _get_long_timeout_params(self, *, multiplier=1):
        connect_params = dict(self.mysql_client_params)
        connect_params["timeout"] = DEFAULT_MYSQL_TIMEOUT * 5 * multiplier
        return connect_params

    def _get_upload_backup_site(self):
        non_recovery_sites = {id: values for id, values in self.backup_sites.items() if not values.get("recovery_only")}
        if not non_recovery_sites:
            raise Exception("No non-recovery sites defined")
        if not self.backup_settings.get("upload_site"):
            if len(non_recovery_sites) > 1:
                raise Exception("No upload site defined but multiple non-recovery sites exist")
            return list(non_recovery_sites.items())[0]

        site_id = self.backup_settings["upload_site"]
        if site_id not in non_recovery_sites:
            raise Exception("Defined upload site not present in list of non-recovery backup sites")
        return site_id, non_recovery_sites[site_id]

    def _get_site_for_stream_id(self, stream_id):
        for backup in self.state["backups"]:
            if backup["stream_id"] == stream_id:
                return backup["site"]
        raise KeyError(f"Stream {stream_id} not found in backups")

    def _get_restore_coordinator_state_file_and_remove_old(self):
        state_file_name = os.path.join(self.state_dir, "restore_coordinator_state.json")
        # If we're retrying restoration there could be an old state file, make sure to delete it
        # so that obsolete state doesn't get reused
        with contextlib.suppress(Exception):
            os.remove(state_file_name)
        return state_file_name

    def _get_restore_coordinator_pending_state_file_and_remove_old(self):
        state_file_name = os.path.join(self.state_dir, "restore_coordinator_state.pending_binlogs")
        # If we're retrying restoration there could be an old state file, make sure to delete it
        # so that obsolete state doesn't get reused
        with contextlib.suppress(Exception):
            os.remove(state_file_name)
        return state_file_name

    def _handle_mode_active(self):
        self._cache_server_uuid_if_missing()
        self._set_uploaded_binlog_references()
        self._refresh_backups_list_and_streams()
        self._mark_periodic_backup_requested_if_interval_exceeded()
        self._create_new_backup_stream_if_requested_and_max_streams_not_exceeded()
        self._update_stream_completed_and_closed_statuses()
        self._rotate_binlog_if_threshold_exceeded()
        self._purge_old_backups()
        self._purge_old_binlogs()
        self._process_local_binlog_updates()
        self._send_binlog_stats()

    def _handle_mode_idle(self):
        self._refresh_backups_list()

    def _handle_mode_observe(self):
        self._cache_server_uuid_if_missing()
        self._refresh_backups_list_and_streams()
        self._purge_old_binlogs()
        self._process_local_binlog_updates()

    def _handle_mode_promote(self):
        self._refresh_backups_list_and_streams()

        # It is possible to have a netsplit where clients can talk to old master and old master
        # can talk to file storage but it cannot talk to standby and other decision making nodes
        # so it gets replaced. In such a situation we want to manually apply any binlogs the old
        # master has managed to upload to storage before making this node applicable for actual
        # promotion.

        if self._prepare_streams_for_promotion() == len(self.backup_streams):
            self.log.info("Switching controller to active mode (1)")
            self.state_manager.update_state(mode=self.Mode.active, promote_details={})
            self._update_mode_tag()
            return

        self._download_unapplied_remote_binlogs()
        self._apply_downloaded_remote_binlogs()
        self._check_binlog_apply_status()

        has_pending = any(
            self.state["promote_details"].get(key) for key in {"binlogs_to_fetch", "binlogs_to_apply", "binlogs_applying"}
        )
        if not has_pending:
            for stream in self.backup_streams:
                if stream.mode == BackupStream.Mode.promoted:
                    stream.activate()

        if self._prepare_streams_for_promotion() == len(self.backup_streams):
            self.log.info("Switching controller to active mode (2)")
            self.state_manager.update_state(mode=self.Mode.active, promote_details={})
            self._update_mode_tag()

    def _handle_mode_restore(self):
        self._create_restore_coordinator_if_missing()
        if self.state["server_uuid"] is None and self.restore_coordinator.server_uuid:
            self.state_manager.update_state(server_uuid=self.restore_coordinator.server_uuid)
        # Need to purge binlogs also during restoration because generating binlogs from relay logs should be enabled
        # also during restoration. The binlogs created during restoration could be required e.g. if an old master has
        # managed to upload binlogs that have not been replicated to a read replica; when the old master gets replaced
        # read replica connects to the new server and must be able to download missing binlogs from there
        self._purge_old_binlogs(mysql_maybe_not_running=True)
        self._process_local_binlog_updates()
        self._extend_binlog_stream_list()
        if self.restore_coordinator.phase == RestoreCoordinator.Phase.failed_basebackup:
            self._switch_basebackup_if_possible()
        if self.state["promote_on_restore_completion"] and self.restore_coordinator.is_complete():
            self.state_manager.update_state(
                # Ensure latest backup list is fetched before promotion so that we
                # start working with appropriate backup streams
                backups_fetched_at=0,
                force_promote=True,
                mode=self.Mode.promote,
                restore_options={},
            )

    def _mark_periodic_backup_requested_if_interval_exceeded(self):
        normalized_backup_time = self._current_normalized_backup_timestamp()
        last_normalized_backup_time = None
        most_recent_scheduled = None
        if self.backup_streams:
            last_normalized_backup_time = max(stream.state["normalized_backup_time"] for stream in self.backup_streams)
            scheduled_streams = [
                stream for stream in self.backup_streams
                if stream.state["backup_reason"] == BackupStream.BackupReason.scheduled
            ]
            if scheduled_streams:
                most_recent_scheduled = max(stream.created_at for stream in scheduled_streams)

        # Don't create new backup unless at least half of interval has elapsed since scheduled last backup. Otherwise
        # we would end up creating a new backup each time when backup hour/minute changes, which is typically undesired.
        # With the "half of interval" check the backup time will quickly drift towards the selected time without backup
        # spamming in case of repeated setting changes.
        half_backup_interval_s = self.backup_settings["backup_interval_minutes"] * 60 / 2
        if (
            last_normalized_backup_time != normalized_backup_time
            and self.state["backup_request"].get("normalized_backup_time") != normalized_backup_time
            and (not most_recent_scheduled or time.time() - most_recent_scheduled >= half_backup_interval_s)
        ):
            self.log.info(
                "New normalized time %r differs from previous %r, adding new backup request", normalized_backup_time,
                last_normalized_backup_time
            )
            self.mark_backup_requested(
                backup_reason=BackupStream.BackupReason.scheduled, normalized_backup_time=normalized_backup_time
            )

    def _prepare_streams_for_promotion(self):
        active_count = 0
        for stream in self.backup_streams:
            if stream.mode == BackupStream.Mode.observe:
                stream.start_preparing_for_promotion()
            elif stream.mode == BackupStream.Mode.promoted:
                self._determine_unapplied_remote_binlogs(stream)
            elif stream.mode == BackupStream.Mode.active:
                active_count += 1
            elif stream.mode != BackupStream.Mode.prepare_promotion:
                raise ValueError(f"Unexpected backup stream mode {stream.mode}")

        return active_count

    def _process_local_binlog_updates(self):
        self.binlog_scanner.scan_new(self._process_new_binlogs)
        self.binlog_scanner.scan_removed(self._process_removed_binlogs)

    def _process_new_binlogs(self, binlogs):
        for stream in self.backup_streams:
            stream.add_binlogs(binlogs)
        self.state_manager.update_state(last_binlog_rotation=time.time())

    def _process_removed_binlogs(self, binlogs):
        if self.mode == self.Mode.observe:
            for stream in self.backup_streams:
                stream.remove_binlogs(binlogs)

    def _purge_old_backups(self):
        purgeable = [backup for backup in self.state["backups"] if backup["completed_at"]]
        if len(purgeable) <= self.backup_settings["backup_count_min"]:
            return

        # For simplicity only ever drop one backup here. This function
        # is called repeatedly so if there are for any reason more backups
        # to drop they will be dropped soon enough
        purgeable = sorted(purgeable, key=lambda info: info["completed_at"])
        backup = purgeable[0]
        if not backup["closed_at"]:
            return

        if time.time() > backup["closed_at"] + self.backup_settings["backup_age_days_max"] * 24 * 60 * 60:
            self.log.info("Backup %r is older than max backup age, dropping it", backup["stream_id"])
        elif len(purgeable) > self.backup_settings["backup_count_max"]:
            self.log.info("Backup count %s is above max allowed, dropping %r", len(purgeable), backup["stream_id"])
        else:
            return

        # This shouldn't happen but better not drop backup that is active
        if any(stream.stream_id == backup["stream_id"] for stream in self.backup_streams):
            self.log.warning("Backup %r to drop is one of active streams, not dropping", backup["stream_id"])

        self._build_backup_stream(backup).remove()
        with self.lock:
            owned_stream_ids = [sid for sid in self.state["owned_stream_ids"] if sid != backup["stream_id"]]
            self.state_manager.update_state(owned_stream_ids=owned_stream_ids)

    def _purge_old_binlogs(self, *, mysql_maybe_not_running=False):
        purge_settings = self.binlog_purge_settings
        if not purge_settings["enabled"] or time.time() - self.state["binlogs_purged_at"] < purge_settings["purge_interval"]:
            return

        binlogs = self.binlog_scanner.binlogs
        backup_streams = self.backup_streams
        replication_state = self.state["replication_state"]
        exclude_uuid = None
        if self.mode != self.Mode.active:
            # If this node is not in active mode disregard any GTIDs with our server UUID when checking whether
            # something has been backed up; anything from us cannot be backed up because we're not the master.
            # Typically there shouldn't be any changes from us either but certain MySQL operations could cause
            # GTID executed to be updated even though no actual changes in database contents are made, which
            # would result in inability to ever purge binlogs because they have GTIDs that other nodes and backups
            # don't have.
            exclude_uuid = self.state["server_uuid"]
        if self.mode == self.Mode.restore:
            # Use more aggressive purge setting when restoring. The backups cannot be in any streams since we
            # don't have streams at this point and we only want to make sure they've been replicated to any
            # other nodes that might belong to the same cluster
            purge_settings = dict(purge_settings)
            purge_settings["purge_when_observe_no_streams"] = True
            purge_settings["min_binlog_age_before_purge"] = purge_settings["min_binlog_age_before_purge"] / 10

        last_purge = self.state["last_binlog_purge"]
        # This is reported in another metric to indicate time since we could've purged binlogs but didn't because
        # there was not sufficient info to make a decision (due to system being idle and no changes happening)
        last_could_have_purged = self.state["last_could_have_purged"]
        try:
            should_purge = self._should_purge_binlogs(
                backup_streams=backup_streams,
                binlogs=binlogs,
                purge_settings=purge_settings,
                replication_state=replication_state,
            )
            if not should_purge:
                # If we didn't purge because we had nothing to do then update the metric to indicate there's no
                # problem with purging
                if not binlogs:
                    last_could_have_purged = time.time()
                return

            binlogs_to_purge, only_binlogs_without_gtids = self.collect_binlogs_to_purge(
                backup_streams=backup_streams,
                binlogs=binlogs,
                exclude_uuid=exclude_uuid,
                log=self.log,
                mode=self.mode,
                purge_settings=purge_settings,
                replication_state=replication_state,
            )
            if not binlogs_to_purge:
                # If we only had binlogs for which we legitimately couldn't tell whether purging was safe or not,
                # update the could have purged timestamp that gets reported as metric data point because for inactive
                # server this is expected behavior and we don't want the metric value to indicate any abnormality
                # in system behavior.
                if only_binlogs_without_gtids:
                    last_could_have_purged = time.time()
            else:
                # PURGE BINARY LOGS TO 'name' does not delete the file identified by 'name' so we need to increase
                # the index by one to get also the last file removed
                base_name, index = binlogs_to_purge[-1]["file_name"].rsplit(".", 1)
                up_until_index = int(index) + 1
                up_until_name = relay_log_name(prefix=base_name, index=up_until_index, full_path=False)
                self.log.info("Purging %s binlogs, up until %r", len(binlogs_to_purge), up_until_name)
                try:
                    with mysql_cursor(**self._get_long_timeout_params()) as cursor:
                        cursor.execute(f"PURGE BINARY LOGS TO '{up_until_name}'")
                except pymysql.err.OperationalError as ex:
                    if mysql_maybe_not_running and ex.args[0] == ERR_CANNOT_CONNECT:
                        self.log.warning("Failed to connect to MySQL to purge binary logs: %r", ex)
                        return
                    if ex.args[0] == ERR_TIMEOUT:
                        # Timeout here doesn't matter much. We'll just retry momentarily
                        self.log.warning("Timeout while purging binary logs: %r", ex)
                        return
                    if ex.args[0] == ERR_CANNOT_CONNECT:
                        # Connection refused doesn't matter much - similar to timeout. We'll retry.
                        self.log.warning("Connection refused while purging binary logs: %r", ex)
                        return
                    raise
                last_purge = time.time()
                last_could_have_purged = last_purge
        finally:
            self.state_manager.update_state(
                binlogs_purged_at=time.time(),
                last_binlog_purge=last_purge,
                last_could_have_purged=last_could_have_purged,
            )
            self.stats.gauge_float("myhoard.binlog.time_since_any_purged", time.time() - last_purge)
            self.stats.gauge_float("myhoard.binlog.time_since_could_have_purged", time.time() - last_could_have_purged)

    def _refresh_backups_list(self):
        interval = self.backup_refresh_interval_base
        if self.mode == self.Mode.active:
            interval *= self.BACKUP_REFRESH_ACTIVE_MULTIPLIER
        if time.time() - self.state["backups_fetched_at"] < interval:
            return None

        backups = self.get_backup_list(
            self.backup_sites, seen_basebackup_infos=self.seen_basebackup_infos, site_transfers=self.site_transfers
        )
        self.state_manager.update_state(backups=backups, backups_fetched_at=time.time())
        return backups

    def _refresh_backups_list_and_streams(self):
        basebackup_streams = {
            stream.stream_id: stream
            for stream in self.backup_streams
            if stream.active_phase == BackupStream.ActivePhase.basebackup
        }

        # Refresh backup streams if backups were refreshed or if we haven't yet created the streams following
        # process restart or sighup (which works by re-creating controller)
        if self._refresh_backups_list() is None and self.backup_streams_initialized:
            return

        backups = self.state["backups"]
        existing_streams = {stream.stream_id: stream for stream in self.backup_streams}
        # Keep any streams that are in basebackup phase because those haven't necessarily
        # yet uploaded any files so the remote backup directory might not exist
        new_streams = basebackup_streams
        for backup in backups:
            stream_id = backup["stream_id"]
            site_info = self.backup_sites.get(backup["site"])
            # We do not create backup streams for recovery sites. Those are only used for restoring
            # basic backup data, never to stream any changes. Also, if config is updated not to
            # contain recovery site anymore we might have backup with no site (because backups list
            # is not forcibly refreshed after reloading config but streams are). Ignore such backups.
            if not site_info or site_info.get("recovery_only"):
                continue
            if stream_id in existing_streams:
                stream = existing_streams[stream_id]
                if stream.mode == BackupStream.Mode.active and stream.active_phase == BackupStream.ActivePhase.none:
                    self.log.info("Stream %s has been closed, joining thread and dropping from list", stream_id)
                    with self.lock:
                        if stream_id in self.state["owned_stream_ids"]:
                            owned_stream_ids = [sid for sid in self.state["owned_stream_ids"] if sid != stream_id]
                            self.state_manager.update_state(owned_stream_ids=owned_stream_ids)
                    stream.join()
                elif stream.mode != BackupStream.Mode.observe or not stream.state["closed_info"]:
                    new_streams[stream_id] = stream
            elif backup["resumable"] and not backup["closed_at"]:
                self.log.info("Starting resumable non-closed stream %r", stream_id)
                new_stream = self._build_backup_stream(backup)
                new_stream.start()
                new_streams[stream_id] = new_stream
            elif not backup["resumable"] and stream_id in self.state["owned_stream_ids"]:
                self.log.warning("Found non-resumable backup %r, removing", backup)
                self._remove_non_resumable_backup(backup)

        removed_stream_ids = set(existing_streams) - set(new_streams)
        for stream_id in removed_stream_ids:
            self.log.info("Stream %r no longer present in backups or became closed, stopping handler", stream_id)
            # This should never happen but if a backup was deleted from external source remove from list of owned ids
            owned_ids = self.state["owned_stream_ids"]
            if stream_id in owned_ids:
                self.log.warning("Unexpected removal of owned stream: %r", stream_id)
                self.state_manager.update_state(owned_stream_ids=[sid for sid in owned_ids if sid != stream_id])
            existing_streams[stream_id].stop()

        self.backup_streams = list(new_streams.values())
        self.backup_streams_initialized = True

    def _relay_log_name(self, *, index, full_path=True):
        return relay_log_name(prefix=self.mysql_relay_log_prefix, index=index, full_path=full_path)

    def _remove_non_resumable_backup(self, backup):
        # Only remove backups we've created. Otherwise it might be non-resumable because
        # the node creating the backup is midway through uploading basebackup files
        stream_id = backup["stream_id"]
        if stream_id not in self.state["owned_stream_ids"]:
            return

        self._build_backup_stream(backup).remove()
        with self.lock:
            owned_stream_ids = [sid for sid in self.state["owned_stream_ids"] if sid != stream_id]
            self.state_manager.update_state(owned_stream_ids=owned_stream_ids)

    def _rotate_binlog(self, *, force_interval=None):
        local_log_index = None
        # FLUSH BINARY LOGS might take a long time if the server is under heavy load,
        # use longer than normal timeout here with multiple retries and increasing timeout.
        for retry, multiplier in [(True, 1), (True, 2), (False, 3)]:
            try:
                with mysql_cursor(**self._get_long_timeout_params(multiplier=multiplier)) as cursor:
                    if force_interval:
                        self.log.info("Over %s seconds elapsed since last new binlog, forcing rotation", force_interval)
                    else:
                        self.log.info("Rotating binlog due to external request")
                    cursor.execute("FLUSH BINARY LOGS")
                    cursor.execute("SHOW BINARY LOGS")
                    log_names = [row["Log_name"] for row in cursor.fetchall()]
                    log_indexes = sorted(int(name.rsplit(".", 1)[-1]) for name in log_names)
                    if len(log_indexes) > 1:
                        # The second last log is the one expected to get backed up. Last one is currently empty
                        local_log_index = log_indexes[-2]
                    self.state_manager.update_state(last_binlog_rotation=time.time())
                    return local_log_index
            except pymysql.err.OperationalError as ex:
                if ex.args[0] == ERR_TIMEOUT:
                    due_to = "timeout"
                elif ex.args[0] == ERR_CANNOT_CONNECT:
                    due_to = "connection refused"
                else:
                    raise ex
                # If this is scheduled rotation we can just ignore the error. The operation will be retried momentarily
                # and if it keeps on failing metric like time since last binlog upload can be used to detect problems
                if force_interval:
                    self.log.warning("Failed to flush binary log due to %s: %r", due_to, ex)
                    return None
                if not retry:
                    raise ex
                self.log.error("Failed to flush binary logs due to %s, retrying: %r", due_to, ex)
        return None

    def _rotate_binlog_if_threshold_exceeded(self):
        # If we haven't seen new binlogs in a while, forcibly flush binlogs so that we upload latest
        # changes. This is to ensure the data that is backed up is never too old without having to
        # process partial files or read the binlog stream dynamically. Note that the rotation is done
        # even when no changes are present to make the binlog rotation behavior predictable and easy
        # to monitor; when the service is operating normally there should always be a binlog upload
        # every ~5 minutes.
        if not any(stream.is_streaming_binlogs() for stream in self.backup_streams):
            # No point in forcing rotation if we have zero backup streams that would be handling the newly generated file
            return
        force_interval = self.backup_settings["forced_binlog_rotation_interval"]
        if time.time() - self.state["last_binlog_rotation"] < force_interval:
            return
        self._rotate_binlog(force_interval=force_interval)

    def _update_mode_tag(self):
        value = self.state["mode"]
        if not isinstance(value, str):
            value = value.value
        self.stats.tags["controller_mode"] = value

    def _send_binlog_stats(self):
        backup_streams = self.backup_streams
        if not backup_streams:
            return

        # Get the min, max and sum of binlog count / bytes for any of the backup streams. We don't want
        # just plain sum because that's often misleading when new backup streams are started. Min tells
        # how much we're behind the actual situation while if max / sum grows one of the streams is
        # lagging behind for one reason or another.
        pending_binlog_counts = list(backup_stream.pending_binlog_count for backup_stream in backup_streams)
        pending_binlog_bytes = list(backup_stream.pending_binlog_bytes for backup_stream in backup_streams)
        self.stats.gauge_int("myhoard.pending_binlog_count_min", min(pending_binlog_counts))
        self.stats.gauge_int("myhoard.pending_binlog_bytes_min", min(pending_binlog_bytes))
        self.stats.gauge_int("myhoard.pending_binlog_count_max", max(pending_binlog_counts))
        self.stats.gauge_int("myhoard.pending_binlog_bytes_max", max(pending_binlog_bytes))
        # Send info about how long it's been since we uploaded binlog
        binlog_upload_ages = [backup_stream.binlog_upload_age for backup_stream in backup_streams]
        self.stats.gauge_float("myhoard.binlog_upload_age_min", min(binlog_upload_ages))
        self.stats.gauge_float("myhoard.binlog_upload_age_max", max(binlog_upload_ages))

    def _set_uploaded_binlog_references(self):
        references = self.state["uploaded_binlogs"]
        for reference in references:
            for stream in self.backup_streams:
                if reference["exclude_stream_id"] != stream.stream_id:
                    stream.add_remote_reference(local_index=reference["local_index"], remote_key=reference["remote_key"])
        # New value may be stored from another thread while we were passing references above. Take a lock to
        # prevent concurrent updates and create new list which does not include any of the entries we just
        # processed.
        processed_references = set((ref["local_index"], ref["remote_key"]) for ref in references)
        with self.lock:
            new_references = [
                ref for ref in self.state["uploaded_binlogs"]
                if (ref["local_index"], ref["remote_key"]) not in processed_references
            ]
            self.state_manager.update_state(uploaded_binlogs=new_references)

    def _should_purge_binlogs(self, *, backup_streams, binlogs, purge_settings, replication_state):
        # If we don't know of any binlogs we obviously have nothing to do/purge
        if not binlogs:
            return False

        if not backup_streams:
            # Don't purge anything if config says purging is not safe when we're in observer mode and there are no
            # backup streams. Depending on the setup this might be fine. E.g. a separate read replica (as opposed to
            # regular standby) would use different backup location than the source service and wouldn't have any
            # backups until a time when it is promoted.
            # In active mode we should always have backup streams but if not don't purge anything.
            purge_when_observe_no_stream = purge_settings["purge_when_observe_no_streams"]
            if (self.mode == self.Mode.observe and not purge_when_observe_no_stream) or self.mode == self.Mode.active:
                self.log.info("No backup streams, not purging binary logs")
                return False

        # If we have been informed of any servers that have no GTIDs whatsoever skip purging because we don't
        # want to purge files when we don't know it's safe.
        for server_name, gtids in replication_state.items():
            if not gtids:
                self.log.info("Server %r has no GTID info available, not purging binary logs", server_name)
                return False

        return True

    def _start_new_backup(self, *, backup_reason, normalized_backup_time):
        stream_id = BackupStream.new_stream_id()
        site_id, backup_site = self._get_upload_backup_site()
        stream = BackupStream(
            backup_reason=backup_reason,
            binlogs=self.binlog_scanner.binlogs,
            compression=backup_site.get("compression"),
            file_storage_setup_fn=lambda: get_transfer(backup_site["object_storage"]),
            file_uploaded_callback=self._binlog_uploaded,
            latest_complete_binlog_index=self.binlog_scanner.latest_complete_binlog_index,
            mode=BackupStream.Mode.active,
            mysql_client_params=self.mysql_client_params,
            mysql_config_file_name=self.mysql_config_file_name,
            mysql_data_directory=self.mysql_data_directory,
            normalized_backup_time=normalized_backup_time,
            rsa_public_key_pem=backup_site["encryption_keys"]["public"],
            server_id=self.server_id,
            site=site_id,
            state_file=self._state_file_from_stream_id(stream_id),
            stats=self.stats,
            stream_id=stream_id,
            temp_dir=self.temp_dir,
        )
        self.backup_streams.append(stream)
        self.state_manager.update_state(
            backup_request={},
            owned_stream_ids=self.state["owned_stream_ids"] + [stream_id],
        )
        stream.start()

    def _state_file_from_stream_id(self, stream_id):
        safe_stream_id = stream_id.replace(":", "_").replace(".", "_")
        return os.path.join(self.state_dir, f"{safe_stream_id}.json")

    def _switch_basebackup_if_possible(self):
        # We're trying to restore a backup but that keeps on failing in the basebackup restoration phase.
        # If we have an older backup available try restoring that and play back all binlogs so that the
        # system should end up in the exact same state eventually.
        backups = sorted(
            (backup for backup in self.state["backups"] if backup["completed_at"]), key=lambda d: d["completed_at"]
        )
        current_stream_id = self.state["restore_options"]["stream_id"]
        earlier_backup = None
        for backup in backups:
            if backup["stream_id"] == current_stream_id:
                break
            earlier_backup = backup
        else:
            raise Exception(f"Stream {current_stream_id} being restored not found in completed backups: {backups}")

        if earlier_backup:
            self.log.info("Earlier backup %r is available, restoring basebackup from that", earlier_backup)
            options = self.state["restore_options"]
            self.restore_coordinator.stop()
            self.state_manager.update_state(restore_options={
                # Get binlogs from all backup streams
                "binlog_streams": [{
                    "site": earlier_backup["site"],
                    "stream_id": earlier_backup["stream_id"],
                }] + options["binlog_streams"],
                "pending_binlogs_state_file": self._get_restore_coordinator_pending_state_file_and_remove_old(),
                "state_file": self._get_restore_coordinator_state_file_and_remove_old(),
                "stream_id": earlier_backup["stream_id"],
                "site": earlier_backup["site"],
                "target_time": options["target_time"],
                "target_time_approximate_ok": options["target_time_approximate_ok"],
            })
            self.restore_coordinator = None
        else:
            # Switch restore coordinator to permanently failed mode
            self.log.info("No earlier basebackup available, cannot recover")
            self.restore_coordinator.update_state(phase=RestoreCoordinator.Phase.failed)

    def _update_stream_completed_and_closed_statuses(self):
        """Mark streams that are catching up with earlier streams as completed when they catch up with the
        stream furthest ahead. When multiple streams are completed mark all but the last one as closed and
        refresh our backup and stream list so that completed ones get removed and new active ones may be
        created when requested."""
        expected_completed = []
        if len(self.backup_streams) == 1:
            if self.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog_catchup:
                self.backup_streams[0].mark_as_completed()
                expected_completed.append(self.backup_streams[0])
        elif self.backup_streams:
            streams_in_catchup = 0
            highest_processed_local_index = -1
            for stream in self.backup_streams:
                if stream.active_phase == BackupStream.ActivePhase.binlog_catchup:
                    streams_in_catchup += 1
                elif (
                    stream.active_phase == BackupStream.ActivePhase.binlog
                    and stream.highest_processed_local_index > highest_processed_local_index
                ):
                    highest_processed_local_index = stream.highest_processed_local_index

            if highest_processed_local_index == -1:
                streams_active = [
                    stream for stream in self.backup_streams if stream.active_phase == BackupStream.ActivePhase.binlog
                ]
                streams_active_no_binlogs = [stream for stream in streams_active if not stream.state["pending_binlogs"]]
                # If we don't have any stream that is in regular binlog streaming phase and we have some
                # that are in catchup phase mark the oldest one as completed or else we can make no progress.
                # This could happen if next backup is started very soon after initial backup, e.g. because
                # the service was started when current time was just before the scheduled backup time
                if streams_in_catchup > 0 and not streams_active:
                    streams = [
                        stream for stream in self.backup_streams
                        if stream.active_phase == BackupStream.ActivePhase.binlog_catchup
                    ]
                    streams = sorted(streams, key=lambda stream: stream.created_at)
                    self.log.info(
                        "Multiple streams exist with none in regular binlog streaming phase, marking %r as complete",
                        streams[0].stream_id
                    )
                    streams[0].mark_as_completed()
                    expected_completed.append(streams[0])
                    self.binlog_not_caught_log_counter = 0
                elif streams_in_catchup > 0 and streams_active_no_binlogs:
                    # Special case: we have zero GTIDs in binlogs so new streams cannot catch up the old one.
                    # Any stream in catchup phase can be immediately marked as completed
                    for stream in self.backup_streams:
                        if stream.active_phase == BackupStream.ActivePhase.binlog_catchup:
                            self.log.info("No local binlogs. Marking stream %r completed", stream.stream_id)
                            stream.mark_as_completed()
                            expected_completed.append(stream)
            else:
                for stream in self.backup_streams:
                    if (
                        stream.active_phase == BackupStream.ActivePhase.binlog_catchup
                        and stream.highest_processed_local_index >= highest_processed_local_index
                    ):
                        self.log.info(
                            "Stream %r caught up with previous stream (%s == %s), marking as completed", stream.stream_id,
                            stream.highest_processed_local_index, highest_processed_local_index
                        )
                        stream.mark_as_completed()
                        expected_completed.append(stream)
                        self.binlog_not_caught_log_counter = 0
                    elif stream.active_phase == BackupStream.ActivePhase.binlog_catchup:
                        # This condition may repeat every iteration (default 1 second) for fairly long time.
                        # Print less frequently to avoid log spam.
                        if self.binlog_not_caught_log_counter % 10 == 0:
                            self.log.info(
                                "Stream %r at position %s while latest position is %s, not yet caught up", stream.stream_id,
                                stream.highest_processed_local_index, highest_processed_local_index
                            )
                        self.binlog_not_caught_log_counter += 1

        max_completion_wait = 2.0
        if expected_completed:
            def is_completed():
                return all(stream.active_phase == BackupStream.ActivePhase.binlog for stream in expected_completed)

            # Wait a moment for the streams to actually get marked as completed;
            # The real update happens on another thread so there is some delay
            if self._wait_for_operation_to_finish(is_completed, wait_time=max_completion_wait):
                self.log.info("All pending mark as completed operations finished")
            else:
                self.log.warning(
                    "Not all streams finished marking themselves completed in %.1f seconds", max_completion_wait
                )

        # Close all but the latest stream in regular binlog streaming phase
        expected_closed = []
        streaming_binlogs = [
            stream for stream in self.backup_streams if stream.active_phase == BackupStream.ActivePhase.binlog
        ]
        if len(streaming_binlogs) > 1:
            streaming_binlogs = sorted(streaming_binlogs, key=lambda s: s.created_at)
            for stream in streaming_binlogs[:-1]:
                self.log.info("Multiple streams in completed state, marking %r as closed", stream.stream_id)
                stream.mark_as_closed()
                expected_closed.append(stream)

        if expected_closed:
            def is_closed():
                return all(stream.active_phase == BackupStream.ActivePhase.none for stream in expected_closed)

            # Wait a moment for the streams to actually get marked as closed;
            # The real update happens on another thread so there is some delay
            if self._wait_for_operation_to_finish(is_closed, wait_time=max_completion_wait):
                self.log.info("All pending mark as closed operations finished")
            else:
                self.log.warning(
                    "Not all streams finished marking themselves closed in %.1f seconds", max_completion_wait
                )

        if not expected_completed and not expected_closed:
            return

        self.state_manager.update_state(backups_fetched_at=0)
        self._refresh_backups_list_and_streams()

    @staticmethod
    def _wait_for_operation_to_finish(check_fn, *, wait_time=2.0):
        # Wait a moment for the streams to actually get marked as completed;
        # The real update happens on another thread so there is some delay
        start_time = time.monotonic()
        while time.monotonic() - start_time < wait_time:
            if check_fn():
                return True
            time.sleep(0.1)

        return False
