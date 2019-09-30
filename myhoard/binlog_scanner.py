# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import logging
import os
import threading
import time

from .append_only_state_manager import AppendOnlyStateManager
from .state_manager import StateManager
from .util import build_gtid_ranges, read_gtids_from_log


class BinlogScanner:
    """Looks for new (complete) and removed binlog files. Scans any new completed files
    for GTID details and maintains disk backed state of all GTID ranges in all complete
    binlogs that still exist on disk."""

    def __init__(self, *, binlog_prefix, server_id, state_file, stats):
        super().__init__()
        binlogs = []
        lock = threading.RLock()
        self.binlog_prefix = binlog_prefix
        binlog_state_name = state_file.replace(".json", "") + ".binlogs"
        self.binlog_state = AppendOnlyStateManager(entries=binlogs, lock=lock, state_file=binlog_state_name)
        self.binlogs = binlogs
        # Keep track of binlogs we have in the file listing local binlogs; if persisting the binlogs
        # succeeds but persisting other metadata fails we'd end up in bad state without this logic
        self.known_local_indexes = {binlog["local_index"] for binlog in self.binlogs}
        self.lock = lock
        self.log = logging.getLogger(self.__class__.__name__)
        self.state = {
            "last_add": time.time(),
            "last_remove": None,
            "next_index": 1,
            "total_binlog_count": 0,
            "total_binlog_size": 0,
        }
        self.state_manager = StateManager(state=self.state, state_file=state_file)
        self.stats = stats
        self.server_id = server_id

    @property
    def latest_complete_binlog_index(self):
        return self.state["next_index"] - 1

    def scan_new(self, added_callback):
        """Scan for any added binlogs. Passes any found binlogs to the given callback function
        before updating internal state."""
        added = []
        last_processed_at = time.time()

        next_index = self.state["next_index"]
        while True:
            # We only scan completed files so expect the index following our next
            # index to be present as well
            if not os.path.exists(self.build_full_name(next_index + 1)):
                self.log.debug("Binlog with index %s not yet present", next_index + 1)
                break

            full_name = self.build_full_name(next_index)
            start_time = time.monotonic()
            gtid_ranges = list(build_gtid_ranges(read_gtids_from_log(full_name)))
            file_size = os.path.getsize(full_name)
            duration = time.monotonic() - start_time
            last_processed_at = time.time()
            binlog_info = {
                "file_size": file_size,
                "full_name": full_name,
                "gtid_ranges": gtid_ranges,
                "local_index": next_index,
                "file_name": os.path.basename(full_name),
                "processed_at": last_processed_at,
                "processing_time": duration,
                "server_id": self.server_id,
            }
            added.append(binlog_info)
            next_index += 1
            self.log.info(
                "New binlog %r (%s bytes) in %.2f seconds, found %d GTID ranges: %r", full_name, file_size, duration,
                len(gtid_ranges), gtid_ranges
            )

        new_size = 0
        if added:
            if added_callback:
                added_callback(added)
            new_size = sum(binlog["file_size"] for binlog in added)
            with self.lock:
                actual_added = [entry for entry in added if entry["local_index"] not in self.known_local_indexes]
                self.binlog_state.append_many(actual_added)
                self.known_local_indexes.update(entry["local_index"] for entry in actual_added)
                self.state_manager.update_state(
                    last_add=last_processed_at,
                    next_index=added[-1]["local_index"] + 1,
                    total_binlog_count=self.state["total_binlog_count"] + len(added),
                    total_binlog_size=self.state["total_binlog_size"] + new_size,
                )
        # Send data points regardless of whether we got any new binlogs
        self.stats.gauge_int("myhoard.binlog.count", self.state["total_binlog_count"])
        self.stats.gauge_int("myhoard.binlog.size", self.state["total_binlog_size"])
        # Track new binlog count and sizes separately to make it possible to get rate of how
        # fast binlogs are being created; if they're being purged quickly then binlog.count
        # and binlog.size may remain relatively unchanged regardless of creation rate
        self.stats.increase("myhoard.binlog.count_new", len(added))
        self.stats.increase("myhoard.binlog.size_new", new_size)
        return added

    def scan_removed(self, removed_callback):
        """Scan for any removed binlogs. Passes any removed binlogs to the given callback function
        before updating internal state."""
        removed_size = 0
        removed = []

        try:
            with self.lock:
                for binlog in self.binlogs:
                    if os.path.exists(binlog["full_name"]):
                        return removed

                    self.log.info("Binlog %r has been removed", binlog["full_name"])
                    removed.append(binlog)
                    removed_size += binlog["file_size"]
            return removed
        finally:
            if removed:
                if removed_callback:
                    removed_callback(removed)
                with self.lock:
                    actual_removed = [
                        binlog for binlog in self.binlogs[:len(removed)] if binlog["local_index"] in self.known_local_indexes
                    ]
                    self.binlog_state.remove_many_from_head(len(actual_removed))
                    for binlog in actual_removed:
                        self.known_local_indexes.discard(binlog["local_index"])
                    self.state_manager.update_state(
                        last_remove=time.time(),
                        total_binlog_count=self.state["total_binlog_count"] - len(removed),
                        total_binlog_size=self.state["total_binlog_size"] - removed_size,
                    )
            self.stats.increase(metric="myhoard.binlog.removed", inc_value=len(removed))
            self.stats.gauge_int("myhoard.binlog.count", self.state["total_binlog_count"])
            self.stats.gauge_int("myhoard.binlog.size", self.state["total_binlog_size"])

    def build_full_name(self, index):
        # Extensions are zero padded to be always at least 6 characters. I.e. file names
        # are prefix.000001, prefix.001000, prefix.100000, prefix.1000000, etc
        return f"{self.binlog_prefix}.{index:06}"
