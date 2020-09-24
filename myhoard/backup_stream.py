# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import contextlib
import enum
import json
import logging
import os
import pymysql
import threading
import time
import uuid
from contextlib import suppress
from datetime import datetime, timezone

from pghoard.rohmu import errors as rohmu_errors
from pghoard.rohmu.compressor import CompressionStream
from pghoard.rohmu.encryptor import EncryptorStream
from pghoard.rohmu.object_storage.s3 import S3Transfer

from .append_only_state_manager import AppendOnlyStateManager
from .basebackup_operation import BasebackupOperation
from .errors import XtraBackupError
from .state_manager import StateManager
from .util import (
    add_gtid_ranges_to_executed_set, are_gtids_in_executed_set, DEFAULT_MYSQL_TIMEOUT, ERR_TIMEOUT,
    first_contains_gtids_not_in_second, make_fs_metadata, mysql_cursor, parse_fs_metadata, parse_gtid_range_string,
    rsa_encrypt_bytes, sort_and_filter_binlogs, track_rate, truncate_gtid_executed
)

BINLOG_BUCKET_SIZE = 500


class BackupStream(threading.Thread):
    """Handles creating a single consistent backup stream. 'stream' here refers to uninterrupted sequence
    of backup data that can be used to restore the system to a consistent state. It includes the basebackup,
    state files and binary logs following the basebackup. There can be multiple simultaneous backup streams:
    when new basebackup is being created an existing stream is still backing up binlogs so that there's no gap
    in PITR capability.

    BackupStream can also be in observer mode, in which case it does not back up anything but just keeps
    track of local and remote state. All streams are started in this mode and once they've updated their
    state they are switched to active mode on master (& read-only replicate) or kept in observer mode on
    standbys. When a node is promoted it goes via prepare_promotion and promoted modes to active mode."""

    ITERATION_SLEEP = 10
    REMOTE_POLL_INTERVAL = 240

    @enum.unique
    class Mode(str, enum.Enum):
        active = "active"
        observe = "observe"
        # Writes promotion info for current server in a safe manner that prevents any incorrect binlogs from
        # being replicated
        prepare_promotion = "prepare_promotion"
        # Whilst in this state the stream does nothing. It expects the external caller to handle its part
        # of promotion preparation and change the stream to active mode once done. The caller needs to ensure
        # any remote binlogs that are not applied locally get applied before switching to active mode
        promoted = "promoted"

    @enum.unique
    class ActivePhase(str, enum.Enum):
        basebackup = "basebackup"
        # Backing up binlogs but we may not yet have reached parity with earlier
        # backup stream so both need to be kept active
        binlog_catchup = "binlog_catchup"
        binlog = "binlog"
        none = "none"

    @enum.unique
    class BackupReason(str, enum.Enum):
        requested = "requested"
        scheduled = "scheduled"

    def __init__(
        self,
        *,
        backup_reason,
        binlogs=None,
        compression=None,
        file_storage_setup_fn,
        file_uploaded_callback=None,
        latest_complete_binlog_index=None,
        mode,
        mysql_client_params,
        mysql_config_file_name,
        mysql_data_directory,
        normalized_backup_time,
        rsa_public_key_pem,
        server_id,
        site,
        state_file,
        stats,
        stream_id=None,
        temp_dir,
    ):
        super().__init__()
        stream_id = stream_id or self.new_stream_id()
        self.basebackup_bytes_uploaded = 0
        self.basebackup_operation = None
        self.basebackup_progress = None
        self.compression = compression
        self.current_upload_index = None
        # This file storage object must only be called from BackupStream's own thread. Calls from
        # other threads must use file_storage_setup_fn to create new file storage instance
        self.file_storage = None
        self.file_storage_setup_fn = file_storage_setup_fn
        self.file_uploaded_callback = file_uploaded_callback
        self.is_running = True
        self.iteration_sleep = BackupStream.ITERATION_SLEEP
        self.known_remote_binlogs = set()
        self.last_basebackup_attempt = None
        self.lock = threading.RLock()
        self.log = logging.getLogger(f"{self.__class__.__name__}/{stream_id}")
        self.mysql_client_params = mysql_client_params
        self.mysql_config_file_name = mysql_config_file_name
        self.mysql_data_directory = mysql_data_directory
        # Keep track of remote binlogs so that we can drop binlogs containing only GTID
        # ranges that have already been backed up from our list of pending binlogs
        remote_binlog_state_name = state_file.replace(".json", "") + ".remote_binlogs"
        remote_binlogs = []
        self.remote_binlog_manager = AppendOnlyStateManager(
            entries=remote_binlogs, lock=self.lock, state_file=remote_binlog_state_name
        )
        self.remote_binlogs = remote_binlogs
        self.known_remote_binlogs = {binlog["remote_index"] for binlog in self.remote_binlogs}
        self.remote_poll_interval = BackupStream.REMOTE_POLL_INTERVAL
        if not isinstance(rsa_public_key_pem, bytes):
            rsa_public_key_pem = rsa_public_key_pem.encode("ascii")
        self.rsa_public_key_pem = rsa_public_key_pem
        self.server_id = server_id
        self.site = site
        if not site:
            raise ValueError("Site cannot be empty")
        active_details = {}
        if mode == self.Mode.active:
            active_details["phase"] = self.ActivePhase.basebackup
        self.state = {
            "active_details": active_details,
            "backup_errors": 0,
            "basebackup_errors": 0,
            "basebackup_file_metadata": None,
            "basebackup_info": {},
            "closed_info": {},
            "completed_info": {},
            "backup_reason": backup_reason,
            "created_at": time.time(),
            "immediate_scan_required": False,
            "initial_latest_complete_binlog_index": latest_complete_binlog_index,
            "last_binlog_upload_time": 0,
            "last_processed_local_index": None,
            "last_remote_state_check": 0,
            # Map of local index to remote key entries for binary logs we have not yet uploaded. Can be used to
            # perform server side copy of the file instead of local upload
            "local_index_to_remote_key": {},
            "mode": mode,
            "next_index": 0,
            "normalized_backup_time": normalized_backup_time,
            "pending_binlogs": list(binlogs) if binlogs else [],
            "prepare_details": {},
            # Set of GTIDs that have been stored persistently to file storage.
            "remote_gtid_executed": {},
            "remote_read_errors": 0,
            "remote_write_errors": 0,
            "stream_id": stream_id,
            "updated_at": time.time(),
            "valid_local_binlog_found": False,
        }
        self.state_manager = StateManager(lock=self.lock, state=self.state, state_file=state_file)
        self.stats = stats
        self.temp_dir = temp_dir
        self.wakeup_event = threading.Event()

    @contextlib.contextmanager
    def running(self):
        """Start the BackupStream thread and shut it down when finished"""
        self.start()
        yield
        self.stop()

    def activate(self):
        with self.lock:
            if self.mode != self.Mode.promoted:
                raise ValueError(f"Only promoted streams can be activated, current mode is {self.mode}")
            phase = self.ActivePhase.binlog if self.state["completed_info"] else self.ActivePhase.binlog_catchup
            self.log.info("Switching stream %r to active mode, phase %s", self.stream_id, phase)
            # Update also last_binlog_upload_time even though we haven't actually uploaded any binlogs
            # yet so that the related metric data point doesn't have invalid value until we've had time
            # to actually upload at least one file
            self.state_manager.update_state(
                mode=self.Mode.active, active_details={"phase": phase}, last_binlog_upload_time=time.time()
            )
            self.wakeup_event.set()

    @property
    def active_phase(self):
        with self.lock:
            if self.mode != self.Mode.active:
                return None
            return self.state["active_details"]["phase"]

    def add_binlogs(self, binlogs):
        if not self.is_immediate_scan_required() and not binlogs:
            return
        # If the stream has been closed we don't care about tracking binlogs for it anymore
        if self.state["closed_info"]:
            return

        with self.lock:
            # If persisting binlog scanner state fails this might get called again with partially same
            # set of binlogs. Filter out any binlogs we already have in our pending list.
            known_indexes = set(binlog["local_index"] for binlog in self.state["pending_binlogs"])
            binlogs = [binlog for binlog in binlogs if binlog["local_index"] not in known_indexes]
            self.state_manager.update_state(
                immediate_scan_required=False,
                pending_binlogs=self.state["pending_binlogs"] + binlogs,
            )
        if binlogs and self.is_streaming_binlogs():
            self.wakeup_event.set()

    def add_remote_reference(self, *, local_index, remote_key):
        with self.lock:
            if local_index in self.state["local_index_to_remote_key"] or local_index == self.current_upload_index:
                return
            if local_index < self.highest_processed_local_index:
                return
            self.log.info(
                "Local binlog %s for stream %r not yet uploaded but another stream has it. Storing remote reference %r",
                local_index, self.stream_id, remote_key
            )
            self.state_manager.update_state(
                local_index_to_remote_key={
                    **self.state["local_index_to_remote_key"], local_index: remote_key
                }
            )

    @property
    def binlog_upload_age(self):
        last_upload = self.state["last_binlog_upload_time"]
        if self.is_streaming_binlogs():
            return time.time() - last_upload
        else:
            # If we're not supposed to be streaming binlogs then report age as 0
            return 0

    @property
    def created_at(self):
        return self.state["created_at"]

    @property
    def highest_processed_local_index(self):
        return self.state["last_processed_local_index"] or -1

    def is_binlog_safe_to_delete(self, binlog, *, exclude_uuid=None):
        """This function is somewhat similar to is_log_backed_up but performs a more advanced check of whether
        a specific binlog, given full log info, has been backed up. This method can be called both on active
        and observe nodes (as opposed to is_log_backed_up, which is only for active streams). This can be used
        to determine whether a specific binary log is safe to delete from backup persistence point-of-view.
        This particular backup stream itself might not yet have made the log part of its remote list of binlogs
        but if some other stream has uploaded the file and this stream is aware of that deletion is safe."""
        # Completed binary logs that existed before this backup stream was originally created
        # cannot be required by us because our basebackup was triggered later
        initial_index = self.state.get("initial_latest_complete_binlog_index")
        if initial_index is not None and binlog["local_index"] <= initial_index:
            return True

        mode = self.mode
        backed_up = False
        if mode == self.Mode.active:
            local_index = binlog["local_index"]
            if local_index < self.highest_processed_local_index:
                backed_up = True
            elif local_index in self.state["local_index_to_remote_key"]:
                backed_up = True
        elif mode == self.Mode.observe:
            # Binlog index is of no use to us. Need to check whether the GTID ranges are available in storage.
            # If the binlog has no GTID ranges then it's not useful for us in any event.
            if not binlog["gtid_ranges"]:
                backed_up = True
            else:
                backed_up = are_gtids_in_executed_set(
                    self.state["remote_gtid_executed"], binlog["gtid_ranges"], exclude_uuid=exclude_uuid
                )

        return backed_up

    def is_immediate_scan_required(self):
        return self.state["immediate_scan_required"]

    def is_log_backed_up(self, *, log_index):
        return log_index <= self.highest_processed_local_index

    def is_streaming_binlogs(self):
        with self.lock:
            if self.mode != BackupStream.Mode.active:
                return False
            phase = self.state["active_details"]["phase"]
            return phase in {BackupStream.ActivePhase.binlog_catchup, BackupStream.ActivePhase.binlog}

    def iterate_remote_binlogs(self, *, reverse=False):
        with self.lock:
            if reverse:
                binlogs = self.remote_binlogs[::-1]
            else:
                binlogs = list(self.remote_binlogs)
        for binlog in binlogs:
            yield binlog

    def mark_as_closed(self):
        if self.state["closed_info"]:
            self.log.warning("Stream %s marked as closed multiple times", self.stream_id)
            return

        self.log.info("Marking stream %s as closed (local update only)", self.stream_id)
        # Just write the closed info to local state here to ensure file storage
        # operation happens from appropriate thread
        self.state_manager.update_state(
            closed_info={
                "closed_at": time.time(),
                "server_id": self.server_id,
            },
        )
        self.wakeup_event.set()

    def _handle_pending_mark_as_closed(self):
        self.log.info("Handling pending mark as closed request for stream %r", self.stream_id)
        closed_info = self.state["closed_info"]
        try:
            key = self._build_full_name("closed.json")
            data = json.dumps(closed_info)
            metadata = make_fs_metadata(closed_info)
            self.file_storage.store_file_from_memory(key, data.encode("utf-8"), metadata=metadata)
            self.state_manager.update_state(active_details={"phase": self.ActivePhase.none})
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Failed to create closed.json")
            self.stats.unexpected_exception(ex=ex, where="BackupStream._handle_pending_mark_as_closed")
            self.state_manager.update_state(remote_write_errors=self.state["remote_write_errors"] + 1)
            self.stats.increase("myhoard.remote_write_errors")

    def mark_as_completed(self):
        if self.state["completed_info"]:
            self.log.warning("Stream %s marked as completed multiple times", self.stream_id)
            return

        self.log.info("Marking stream %r as completed (local update only)", self.stream_id)
        # Just write the completed info to local state here to ensure file storage
        # operation happens from appropriate thread
        self.state_manager.update_state(
            completed_info={
                "completed_at": time.time(),
                "server_id": self.server_id,
            },
        )
        self.wakeup_event.set()

    def _handle_pending_mark_as_completed(self):
        self.log.info("Handling pending mark as completed request for stream %r", self.stream_id)
        completed_info = self.state["completed_info"]
        try:
            key = self._build_full_name("completed.json")
            data = json.dumps(completed_info)
            metadata = make_fs_metadata(completed_info)
            self.file_storage.store_file_from_memory(key, data.encode("utf-8"), metadata=metadata)
            self.state_manager.update_state(active_details={"phase": self.ActivePhase.binlog})
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Failed to create completed.json")
            self.stats.unexpected_exception(ex=ex, where="BackupStream._handle_pending_mark_as_completed")
            self.state_manager.update_state(remote_write_errors=self.state["remote_write_errors"] + 1)
            self.stats.increase("myhoard.remote_write_errors")

    @property
    def mode(self):
        return self.state["mode"]

    @staticmethod
    def new_stream_id():
        return f"{datetime.now(timezone.utc).isoformat()}Z_{uuid.uuid4()}"

    @property
    def pending_binlog_bytes(self):
        with self.lock:
            return sum(binlog["file_size"] for binlog in self.state["pending_binlogs"])

    @property
    def pending_binlog_count(self):
        with self.lock:
            return len(self.state["pending_binlogs"])

    def remove(self):
        self.stop()
        file_storage = self.file_storage_setup_fn()
        self.state_manager.delete_state()
        try:
            file_storage.delete_tree(f"{self.site}/{self.stream_id}")
        except FileNotFoundError:
            pass
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error("Removing remote backup failed: %r", ex)
            self.stats.unexpected_exception(ex=ex, where="BackupStream.remove")

    def remove_binlogs(self, binlogs):
        # If the stream has been closed we don't care about tracking binlogs for it anymore
        if not binlogs or self.state["closed_info"]:
            return

        with self.lock:
            # If persisting binlog scanner state fails this might get called again with partially same
            # set of binlogs and this particular stream might not know about the binlogs that were removed
            # in the first place. Check which ones actually are in the beginning of our list of known
            # binlogs and remove those.
            removed_indexes = set(binlog["local_index"] for binlog in binlogs)
            remove_count = 0
            for binlog in self.state["pending_binlogs"]:
                if binlog["local_index"] in removed_indexes:
                    remove_count += 1
                else:
                    break
            if remove_count > 0:
                self.log.info("Removed %s binlogs from stream %r", remove_count, self.stream_id)
                self.state_manager.update_state(pending_binlogs=self.state["pending_binlogs"][remove_count:])

    def run(self):
        self.log.info("Backup stream %r running", self.stream_id)
        while self.is_running:
            try:
                if not self.file_storage:
                    self.file_storage = self.file_storage_setup_fn()
                if self.mode == self.Mode.active:
                    if self.state["completed_info"] and self.active_phase == self.ActivePhase.binlog_catchup:
                        self._handle_pending_mark_as_completed()
                    if self.state["closed_info"] and self.active_phase != self.ActivePhase.none:
                        self._handle_pending_mark_as_closed()
                    if self.active_phase == self.ActivePhase.basebackup:
                        self._take_basebackup()
                    if self.is_streaming_binlogs():
                        self._upload_binlogs()
                    if self.active_phase == self.ActivePhase.none:
                        self.log.info(
                            "Backup stream %s is closed, stopping stream processor as no updates are possible",
                            self.stream_id
                        )
                        self.is_running = False
                        break
                elif self.mode == self.Mode.observe:
                    self._check_remote_state()
                elif self.mode == self.Mode.prepare_promotion:
                    self._prepare_promotion()

                self.wakeup_event.wait(self._get_iteration_sleep())
                self.wakeup_event.clear()
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unexpected exception in mode %s", self.mode)
                self.stats.unexpected_exception(ex=ex, where="BackupStream.run")
                self.state_manager.increment_counter(name="backup_errors")
                self.stats.increase("myhoard.backup_stream.errors")
                time.sleep(1)

    def start_preparing_for_promotion(self):
        with self.lock:
            if self.mode != BackupStream.Mode.observe:
                raise ValueError("Invalid request to prepare promotion while in mode {}".format(self.mode))

            self.state_manager.update_state(mode=self.Mode.prepare_promotion)
            self.wakeup_event.set()

    def stop(self):
        self.log.info("Stopping backup stream")
        self.is_running = False
        self.wakeup_event.set()
        op = self.basebackup_operation
        if op:
            op.abort("BackupStream stop requested")
        # Thread might not have been started or could've already been joined, we don't care about that
        with suppress(Exception):
            self.join()
        self.log.info("Backup stream stopped")

    @property
    def stream_id(self):
        return self.state["stream_id"]

    def _basebackup_progress_callback(self, **kwargs):
        self.basebackup_progress = kwargs

    def _basebackup_stream_handler(self, stream):
        file_storage = self.file_storage_setup_fn()

        metadata = make_fs_metadata({
            "backup_started_at": time.time(),
            "uploaded_from": self.server_id,
        })

        last_time = [time.monotonic()]
        last_value = [0]
        self.basebackup_bytes_uploaded = 0

        def upload_progress(bytes_sent):
            self.basebackup_bytes_uploaded = bytes_sent
            # Track both absolute number and explicitly calculated rate. The rate can be useful as
            # a separate measurement because uploads are not ongoing all the time and calculating
            # rate based on raw byte counter requires knowing when the operation started and ended
            self.stats.gauge_int("myhoard.backup_stream.basebackup_bytes_uploaded", self.basebackup_bytes_uploaded)
            last_value[0], last_time[0] = track_rate(
                current=bytes_sent,
                last_recorded=last_value[0],
                last_recorded_time=last_time[0],
                metric_name="myhoard.backup_stream.basebackup_upload_rate",
                stats=self.stats,
            )

        file_storage.store_file_object(
            self._build_full_name("basebackup.xbstream"), stream, metadata=metadata, upload_progress_fn=upload_progress
        )
        self.state_manager.update_state(basebackup_file_metadata=metadata)

    def _build_full_name(self, name):
        return f"{self.site}/{self.stream_id}/{name}"

    def _cache_basebackup_info(self):
        if self.state["basebackup_info"]:
            return True

        try:
            info_str, _ = self.file_storage.get_contents_to_string(self._build_full_name("basebackup.json"))
            basebackup_info = json.loads(info_str)
            self.state_manager.update_state(
                basebackup_info=basebackup_info,
                backup_reason=basebackup_info["backup_reason"],
                created_at=basebackup_info["initiated_at"],
                normalized_backup_time=basebackup_info["normalized_backup_time"],
                remote_gtid_executed=basebackup_info["gtid_executed"],
            )
            return True
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Failed to get basebackup info")
            self.stats.unexpected_exception(ex=ex, where="BackupStream._cache_basebackup_info")
            self.state_manager.increment_counter(name="remote_read_errors")
            self.stats.increase("myhoard.remote_read_errors")
            return False

    def _cache_file_metadata(self, *, missing_ok=True, remote_name, setting_name):
        if self.state[setting_name]:
            return True

        try:
            metadata = self.file_storage.get_metadata_for_key(self._build_full_name(remote_name))
            self.state_manager.update_state(**{setting_name: parse_fs_metadata(metadata)})
            return True
        except Exception as ex:  # pylint: disable=broad-except
            if missing_ok and isinstance(ex, rohmu_errors.FileNotFoundFromStorageError):
                return False
            self.log.exception("Failed to get metadata for %s", remote_name)
            self.stats.unexpected_exception(ex=ex, where="BackupStream._cache_file_metadata")
            self.state_manager.increment_counter(name="remote_read_errors")
            self.stats.increase("myhoard.remote_read_errors")
            raise

    def _cache_remote_binlogs(self, *, ignore_own_promotion):
        next_index = self.state["next_index"] or 1
        last_index = next_index - 1
        bucket = next_index // BINLOG_BUCKET_SIZE
        new_binlogs = []
        highest_index = 0
        start_time = time.monotonic()

        while True:
            try:
                binlogs, highest_index = self._list_new_binlogs_in_bucket(
                    bucket, highest_index=highest_index, next_index=next_index
                )
                if not binlogs:
                    break
                new_binlogs.extend(binlogs)
                if (highest_index + 1) % BINLOG_BUCKET_SIZE == 0:
                    bucket += 1
                    next_index = highest_index + 1
                else:
                    break
            except rohmu_errors.FileNotFoundFromStorageError:
                break
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Failed to list remote binlogs")
                self.stats.unexpected_exception(ex=ex, where="BackupStream._cache_remote_binlogs")
                self.state_manager.increment_counter(name="remote_read_errors")
                self.stats.increase("myhoard.remote_read_errors")
                raise

        promotions = self._get_promotions(ignore_own_promotion=ignore_own_promotion)

        duration = time.monotonic() - start_time
        self.log.info("Retrieved %s binlogs in %.2f seconds", len(new_binlogs), duration)
        if new_binlogs:
            new_binlogs = sort_and_filter_binlogs(
                binlogs=new_binlogs, last_index=last_index, log=self.log, promotions=promotions
            )
        if new_binlogs:
            # If latter state update fails we'll already have some of the binlogs to first state file
            actual_new_binlogs = [
                binlog for binlog in new_binlogs if binlog["remote_index"] not in self.known_remote_binlogs
            ]
            # Also update info regarding which GTIDs are included in the backup
            gtid_executed = self.state["remote_gtid_executed"]
            new_gtid_ranges = [binlog["gtid_ranges"] for binlog in new_binlogs if binlog["gtid_ranges"]]
            if new_gtid_ranges:
                gtid_executed = add_gtid_ranges_to_executed_set(gtid_executed, *new_gtid_ranges)
            self.remote_binlog_manager.append_many(actual_new_binlogs)
            self.known_remote_binlogs.update(binlog["remote_index"] for binlog in actual_new_binlogs)
            self.state_manager.update_state(
                next_index=new_binlogs[-1]["remote_index"] + 1, remote_gtid_executed=gtid_executed
            )

    def _check_remote_state(self, *, force=False, ignore_own_promotion=False):
        last_check = self.state["last_remote_state_check"]
        # Don't fetch remote state if we've already done so or we're in active
        # backup mode, in which case we don't expect external changes
        if not force and (time.time() - last_check < self.remote_poll_interval):
            return

        self._cache_file_metadata(
            missing_ok=False, remote_name="basebackup.xbstream", setting_name="basebackup_file_metadata"
        )
        self._cache_basebackup_info()
        self._cache_file_metadata(remote_name="completed.json", setting_name="completed_info")
        self._cache_file_metadata(remote_name="closed.json", setting_name="closed_info")
        self._cache_remote_binlogs(ignore_own_promotion=ignore_own_promotion)

        self.state_manager.update_state(last_remote_state_check=time.time())

    def _get_iteration_sleep(self):
        # Sleep less when in prepare_promotion mode because this should complete as soon as
        # possible to reduce downtime
        if self.mode == self.Mode.prepare_promotion:
            return self.iteration_sleep / 10.0
        else:
            return self.iteration_sleep

    def _get_promotions(self, *, ignore_own_promotion):
        promotions = {}
        try:
            for info in self.file_storage.list_iter(self._build_full_name("promotions")):
                # There could theoretically be multiple promotions with the same
                # index value if new master got promoted but then failed before
                # managing to upload any binlogs. To cope with that only keep one
                # promotion info per server id (the one with most recent timestamp)
                info = parse_fs_metadata(info["metadata"])
                if ignore_own_promotion and info["server_id"] == self.server_id:
                    continue
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
            return {start_index: info["server_id"] for start_index, info in promotions.items()}
        except Exception as ex:  # pylint: disable=broad-except
            # There should always be one promotion file so file not found is real error too
            self.log.exception("Failed to list promotions")
            self.stats.unexpected_exception(ex=ex, where="BackupStream._get_promotions")
            self.state_manager.increment_counter(name="remote_read_errors")
            self.stats.increase("myhoard.remote_read_errors")
            raise

    def _get_remote_basebackup_metadata(self):
        if self.state["basebackup_file_metadata"]:
            return True

        try:
            metadata = self.file_storage.get_metadata_for_key(self._build_full_name("basebackup.xbstream"))
            self.state_manager.update_state(basebackup_file_metadata=parse_fs_metadata(metadata))
            return True
        except rohmu_errors.FileNotFoundFromStorageError:
            self.state_manager.update_state(last_remote_state_check=time.time())
            return True
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error("Failed to get metadata for basebackup: %r", ex)
            self.stats.unexpected_exception(ex=ex, where="BackupStream._get_remote_basebackup_metadata")
            self.state_manager.increment_counter(name="remote_read_errors")
            self.stats.increase("myhoard.remove_read_errors")
            return False

    def _list_new_binlogs_in_bucket(self, bucket, *, highest_index, next_index):
        """Most of the time there are no new files. Using list_iter with metadata is heavy for S3 so if file
        storage is S3 start by listing files without metadata and only fetch metadata for the new files."""
        full_bucket_name = self._build_full_name(f"binlogs/{bucket}")
        # If we're at the beginning of the bucket then just get everything with metadata
        # because our conditional metadata fetching optimization cannot make this any faster
        if self._should_list_with_metadata(next_index=next_index):
            candidates = list(self.file_storage.list_iter(full_bucket_name, with_metadata=True))
        else:
            all_wo_metadata = self.file_storage.list_iter(full_bucket_name, with_metadata=False)
            candidates = []
            for info in all_wo_metadata:
                remote_index = int(info["name"].rsplit("/", 1)[-1].split("_", 1)[0])
                if remote_index < next_index:
                    continue
                info["metadata"] = self.file_storage.get_metadata_for_key(info["name"])
                candidates.append(info)

        new_binlogs = []
        for info in candidates:
            binlog = parse_fs_metadata(info["metadata"])
            binlog["remote_file_size"] = info["size"]
            highest_index = max(highest_index, binlog["remote_index"])
            if binlog["remote_index"] < next_index:
                continue
            new_binlogs.append(binlog)
        return new_binlogs, highest_index

    def _mark_upload_completed(self, *, binlog, upload_time):
        with self.lock:
            remote_keys = self.state["local_index_to_remote_key"]
            if binlog["local_index"] in remote_keys:
                remote_keys = {key: value for key, value in remote_keys.items() if key != binlog["local_index"]}
            self.state_manager.update_state(
                last_binlog_upload_time=upload_time,
                last_processed_local_index=binlog["local_index"],
                local_index_to_remote_key=remote_keys,
                next_index=binlog["remote_index"] + 1,
                pending_binlogs=self.state["pending_binlogs"][1:],
            )

    def _prepare_promotion(self):
        # Need to perform two-phased creation of promotion file:
        # 1. Update state
        # 2. Create promotion file with the next_index from step 1
        # 3. Update state but ignore our promotion file so that possible updates from old master are included
        #    in the list of valid binlogs
        # 4. Update promotion file with the next_index from step 3
        # If we only did the first iteration (steps 1 & 2) then possible other standbys could end up updating
        # their state between steps 1 and 2 and there could be some binlogs from old master that they apply because
        # our promotion file isn't present yet.
        # This logic does not introduce reverse race condition because even if our promotion file from step 2
        # causes the standbys to ignore binlogs that they should apply they will correctly include them when
        # they update their state the next time after step 4.
        prepare_details = self.state["prepare_details"]
        if not prepare_details.get("initial_promo_index"):
            self._check_remote_state(force=True)
            self._write_promotion_info(promoted_at=time.time(), start_index=self.state["next_index"])
            self.state_manager.update_state(
                prepare_details=dict(prepare_details, initial_promo_index=self.state["next_index"])
            )
        prepare_details = self.state["prepare_details"]
        if not prepare_details.get("final_promo_index"):
            self._check_remote_state(force=True, ignore_own_promotion=True)
            if self.state["next_index"] != prepare_details["initial_promo_index"]:
                self._write_promotion_info(promoted_at=time.time(), start_index=self.state["next_index"])
            self.state_manager.update_state(
                prepare_details=dict(prepare_details, final_promo_index=self.state["next_index"])
            )
        self.state_manager.update_state(mode=self.Mode.promoted, prepare_details={})

    def _should_list_with_metadata(self, *, next_index):
        return next_index % BINLOG_BUCKET_SIZE == 0 or next_index == 1 or not isinstance(self.file_storage, S3Transfer)

    def _take_basebackup(self):
        if self.last_basebackup_attempt is not None:
            fail_count = self.state["basebackup_errors"]
            # Max 14 means we'll eventually retry ~ every 50 minutes
            wait_multiplier = 1.5 ** min(fail_count, 14)
            total_wait = self.iteration_sleep * wait_multiplier
            elapsed = time.monotonic() - self.last_basebackup_attempt
            if elapsed < total_wait:
                self.log.info(
                    "Basebackup has failed %r times previously. Delay until next attempt is %.1f, only %.1f elapsed so far",
                    fail_count, total_wait, elapsed
                )
                return

        start_time = time.time()
        encryption_key = os.urandom(24)
        self.basebackup_operation = BasebackupOperation(
            encryption_algorithm="AES256",
            encryption_key=encryption_key,
            mysql_client_params=self.mysql_client_params,
            mysql_config_file_name=self.mysql_config_file_name,
            mysql_data_directory=self.mysql_data_directory,
            progress_callback=self._basebackup_progress_callback,
            stats=self.stats,
            stream_handler=self._basebackup_stream_handler,
            temp_dir=self.temp_dir,
        )
        try:
            self.basebackup_operation.create_backup()
            # Must flush binlogs immediately after taking basebackup to ensure the last
            # binlog that was being written while basebackup was created is backed up.
            # This is to ensure the binlog is immediately backed up and available for
            # patching gtid_executed even if restoration is performed to the time when
            # basebackup was created.
            # FLUSH BINARY LOGS might take a long time if the server is under heavy load,
            # use longer than normal timeout here with multiple retries and increasing timeout.
            connect_params = dict(self.mysql_client_params)
            for retry, multiplier in [(True, 1), (True, 2), (False, 3)]:
                try:
                    connect_params["timeout"] = DEFAULT_MYSQL_TIMEOUT * 5 * multiplier
                    with mysql_cursor(**connect_params) as cursor:
                        cursor.execute("FLUSH BINARY LOGS")
                        cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
                        gtid_executed = parse_gtid_range_string(cursor.fetchone()["gtid_executed"])
                    break
                except pymysql.err.OperationalError as ex:
                    if not retry or ex.args[0] != ERR_TIMEOUT:
                        raise ex
                    self.log.error("Failed to flush binary logs due to timeout, retrying: %r", ex)

            # Add one second to ensure any transactions in the binary log we flushed
            # above must be played back when the backup is restored (as restoring to
            # earlier time is disallowed) to ensure the gtid_executed patching happens
            end_time = time.time() + 1

            compressed_size = self.file_storage.get_file_size(self._build_full_name("basebackup.xbstream"))

            # Write promotion info so that determining correct binlog sequence does not
            # require special logic for figuring out correct server id at the beginning
            self._write_promotion_info(promoted_at=end_time, start_index=1)

            binlog_info = self.basebackup_operation.binlog_info
            # Figure out the full GTID executed value for the basebackup (i.e. all GTIDs that are included in it).
            # It must be a subset of the gtid_executed we fetched from DB after the basebackup completed. As a
            # special case it can also be empty if this is an empty server that has not produced any GTIDs yet.
            # The full value is required to keep track of what GTIDs are included in a specific backup so that
            # we know which binary logs are safe to purge and which aren't.
            last_gtid = binlog_info["gtid"] if binlog_info else None
            if last_gtid:
                self.log.info("Last basebackup GTID %r, truncating GTID executed %r accordingly", last_gtid, gtid_executed)
                truncate_gtid_executed(gtid_executed, last_gtid)

            info = {
                "binlog_index": int(binlog_info["file_name"].split(".")[-1]) if binlog_info else None,
                "binlog_name": binlog_info["file_name"] if binlog_info else None,
                "binlog_position": binlog_info["file_position"] if binlog_info else None,
                "backup_reason": self.state["backup_reason"],
                "compressed_size": compressed_size,
                "encryption_key": rsa_encrypt_bytes(self.rsa_public_key_pem, encryption_key).hex(),
                "end_size": self.basebackup_operation.data_directory_size_end,
                "end_ts": end_time,
                "gtid": last_gtid,
                "gtid_executed": gtid_executed,
                "initiated_at": self.created_at,
                "lsn_info": self.basebackup_operation.lsn_info,
                "normalized_backup_time": self.state["normalized_backup_time"],
                "number_of_files": self.basebackup_operation.number_of_files,
                "start_size": self.basebackup_operation.data_directory_size_start,
                "start_ts": start_time,
                "uploaded_from": self.server_id,
            }
            self.file_storage.store_file_from_memory(
                self._build_full_name("basebackup.json"),
                json.dumps(info).encode("utf-8"),
            )
            self.state_manager.update_state(
                active_details={"phase": self.ActivePhase.binlog_catchup},
                basebackup_info=info,
                # We want local binlogs to be scanned asap so that we can start uploading those
                immediate_scan_required=True,
                # Update to prevent bad metrics data points until we've had time to upload first binlog
                last_binlog_upload_time=time.time(),
                remote_gtid_executed=gtid_executed,
            )
            uncompressed_size = self.basebackup_operation.data_directory_size_end
            self.stats.gauge_int("myhoard.basebackup.bytes_uncompressed", uncompressed_size)
            self.stats.gauge_int("myhoard.basebackup.bytes_compressed", compressed_size)
            if uncompressed_size and compressed_size:
                self.stats.gauge_float("myhoard.basebackup.compression_ratio", uncompressed_size / compressed_size)
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Failed to take basebackup")
            self.stats.increase("myhoard.basebackup.errors")
            self.state_manager.increment_counter(name="basebackup_errors")
            self.last_basebackup_attempt = time.monotonic()
            # If this is not a remote failure error (i.e. any type of error arising from programming
            # issues in Python then we want to log this as an unexpected exception so it'll dispatch
            # to metrics and Sentry etc.
            if not isinstance(ex, XtraBackupError):
                self.stats.unexpected_exception(ex=ex, where="BackupStream._take_basebackup")
        finally:
            self.basebackup_operation = None

    def _upload_binlogs(self):
        while True:
            with self.lock:
                pending_binlogs = self.state["pending_binlogs"]
                if not pending_binlogs:
                    return
                binlog = pending_binlogs[0]
                skip = True
                if self.state["valid_local_binlog_found"]:
                    # Once we've uploaded one local binlog we'll want to upload all that follow
                    skip = False
                elif self.state["basebackup_info"]["uploaded_from"] == self.server_id:
                    # If we're the server that created this backup stream exclude based on basebackup binlog info;
                    # if this binlog preceeds the point that is already included in basebackup then don't upload
                    if (
                        self.state["basebackup_info"]["binlog_index"] is not None
                        and binlog["local_index"] < self.state["basebackup_info"]["binlog_index"]
                    ):
                        self.log.info("Skipping upload of binlog %s because it predates basebackup", binlog["local_index"])
                    else:
                        skip = False
                        self.state_manager.update_state(valid_local_binlog_found=True)
                elif binlog["gtid_ranges"]:
                    # Does this binlog have some GTID ranges that are not included in the latest remote binlog with GTIDs?
                    remote_with_gtids = None
                    for remote_binlog in reversed(self.remote_binlogs):
                        if remote_binlog["gtid_ranges"]:
                            remote_with_gtids = remote_binlog
                            break
                    if not remote_with_gtids:
                        # This is something that is not really expected to happen but if it does presumably our
                        # binlog is valid and we should upload it
                        self.log.info(
                            "Found no remote binlogs with GTIDs, assuming local binlog %s is valid", binlog["local_index"]
                        )
                        skip = False
                    elif first_contains_gtids_not_in_second(binlog["gtid_ranges"], remote_with_gtids["gtid_ranges"]):
                        # We have some GTIDs that are not backed up so back up this binlog. Note that this binlog
                        # may have partially same GTIDs that are already included in the remote binlog (or some
                        # earlier remote binlog) but that does not matter as the MySQL server will just skip over
                        # any GTIDs it has already processed. Technically we wouldn't even need this entire logic of
                        # excluding duplicate binlogs but it would be wasteful to upload potentially very large number
                        # of files that have no new GTIDs compared to existing remote state.
                        self.log.info(
                            "Local binlog %r contains GTIDs not included in last valid remote binlog %r, uploading", binlog,
                            remote_with_gtids
                        )
                        skip = False
                    if not skip:
                        self.state_manager.update_state(valid_local_binlog_found=True)
                else:
                    self.log.info(
                        "Local binlog %r contains no GTID ranges and we haven't uploaded any binlogs with GTIDs since "
                        "promotion. Cannot determine whether the file is needed or not -> skipping", binlog
                    )

                if skip:
                    pending_binlogs.pop(0)
                    self.state_manager.update_state(pending_binlogs=pending_binlogs)
                    continue

            if not self._upload_binlog(binlog):
                return

    def _upload_binlog(self, binlog):
        next_index = self.state["next_index"]
        if next_index == 0:
            # Newly promoted master will as part of the promotion sequence figure out which was the last
            # binlog file the old master uploaded that is considered valid. That promotion code is then
            # expected to explicitly set our value so that we create uninterrupted sequence of ids that
            # can be easily played back using the promotion hints that are stored separately
            if self.remote_binlogs:
                raise Exception("next_index must be explicitly specified upon promotion")

            next_index = 1

        # Name the file <next_index>_<server_id> because we cannot guarantee old master won't
        # upload files that should not be part of the recoverable binlog stream and if server id
        # was omitted old master could overwrite critical data new master just uploaded.
        # Also, put the binlogs into buckets of BINLOG_BUCKET_SIZE entries to allow more efficient
        # listing of available binlogs while restoring very large stream. (Only BINLOG_BUCKET_SIZE
        # minus one in first bucket since index starts from 1)
        bucket = next_index // BINLOG_BUCKET_SIZE
        index_name = self._build_full_name(f"binlogs/{bucket}/{next_index}_{self.server_id}")
        # pylint: disable=consider-using-ternary
        compression_algorithm = (self.compression and self.compression.get("algorithm")) or "snappy"
        compression_level = (self.compression and self.compression.get("level")) or 0
        binlog = dict(binlog)
        binlog["remote_index"] = next_index
        binlog["remote_key"] = index_name
        binlog["compression_algorithm"] = compression_algorithm
        metadata = make_fs_metadata(binlog)
        self.current_upload_index = binlog["local_index"]
        try:
            start = time.monotonic()
            start_wall = time.time()
            # We might have already uploaded this file but just partially failed to update state. Check if that's the case
            if next_index in self.known_remote_binlogs:
                self.log.warning("Binary log %r seems to have already been uploaded by us", binlog)
                self._mark_upload_completed(binlog=binlog, upload_time=start_wall)
            else:
                log_action = "uploaded local"
                existing_remote_key = self.state["local_index_to_remote_key"].get(self.current_upload_index)
                if existing_remote_key:
                    log_action = "remote copied"
                    self.file_storage.copy_file(
                        source_key=existing_remote_key, destination_key=binlog["remote_key"], metadata=metadata
                    )
                    self.stats.increase("myhoard.binlog.remote_copy")
                else:
                    with open(binlog["full_name"], "rb") as input_file:
                        compress_stream = CompressionStream(input_file, compression_algorithm, compression_level)
                        encrypt_stream = EncryptorStream(compress_stream, self.rsa_public_key_pem)
                        self.file_storage.store_file_object(index_name, encrypt_stream, metadata=metadata)
                        self.stats.increase("myhoard.binlog.upload")
                    if self.file_uploaded_callback:
                        self.file_uploaded_callback(
                            local_index=binlog["local_index"], remote_key=binlog["remote_key"], stream=self
                        )
                with self.lock:
                    self.remote_binlog_manager.append(binlog)
                    self.known_remote_binlogs.add(binlog["remote_index"])
                    self._mark_upload_completed(binlog=binlog, upload_time=start_wall)
                elapsed = time.monotonic() - start
                self.log.info(
                    "Successfully %s binlog index %s as remote index %s in %.1f seconds", log_action, binlog["local_index"],
                    binlog["remote_index"], elapsed
                )
            return True
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Failed to upload binlog %r", binlog)
            self.stats.unexpected_exception(ex=ex, where="BackupStream._upload_binlog")
            self.state_manager.increment_counter(name="remote_write_errors")
            self.stats.increase("myhoard.binlog.upload_errors")
            return False
        finally:
            self.current_upload_index = None

    def _write_promotion_info(self, *, promoted_at, start_index):
        promotion_info = {
            "promoted_at": promoted_at,
            "server_id": self.server_id,
            "start_index": start_index,
        }
        # Store file contents as metadata to make accessing it easier
        metadata = make_fs_metadata(promotion_info)
        self.file_storage.store_file_from_memory(
            self._build_full_name(f"promotions/{self.server_id}"),
            json.dumps(promotion_info).encode("utf-8"),
            metadata=metadata,
        )
