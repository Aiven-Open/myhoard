# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from .errors import DiskFullError
from .util import (
    CHECKPOINT_FILENAME,
    find_extra_xtrabackup_executables,
    get_xtrabackup_version,
    parse_version,
    parse_xtrabackup_info,
)
from contextlib import suppress
from rohmu.util import increase_pipe_capacity, set_stream_nonblocking
from typing import Callable, Dict, Final, Optional

import base64
import fnmatch
import logging
import multiprocessing
import os
import re
import select
import shutil
import subprocess
import tempfile
import threading


class BasebackupRestoreOperation:
    """Restores an existing basebackup. Note that the stream handler callback is
    executed on another thread so that the main thread can manage restore state
    information while the data is being loaded."""

    #: Error message from xbstream / xtrabackup when disk is full.
    NO_SPACE_LEFT_ON_DEVICE: Final[str] = "OS errno 28 - No space left on device"

    prepare_completed_re = re.compile(r"Shutdown completed; log sequence number (\d+)$")

    # Captures the currently-recovered LSN from the canonical InnoDB scan line, e.g.
    #   "InnoDB: Doing recovery: scanned up to log sequence number 49787916061696"
    # This is the only line we rely on for prepare progress: it is emitted
    # repeatedly during recovery and its LSN monotonically advances towards last_lsn
    # (the redo stop LSN). See prepare_last_lsn for why it's last_lsn and not to_lsn.
    prepare_lsn_re = re.compile(r"Doing recovery:\s*scanned up to log sequence number\s*(\d+)")

    def __init__(
        self,
        *,
        encryption_algorithm,
        encryption_key,
        free_memory_percentage,
        mysql_config_file_name,
        mysql_data_directory,
        stats,
        stream_handler,
        target_dir: str,
        temp_dir: str,
        backup_tool_version: str | None = None,
    ):
        self.current_file = None
        self.data_directory_size_end = None
        self.data_directory_size_start = None
        self.encryption_algorithm = encryption_algorithm
        self.encryption_key = encryption_key
        self.free_memory_percentage: Optional[int] = free_memory_percentage
        self.log = logging.getLogger(self.__class__.__name__)
        self.mysql_config_file_name = mysql_config_file_name
        self.mysql_data_directory = mysql_data_directory
        self.number_of_files = 0
        self.prepared_lsn = None
        self.proc: subprocess.Popen[bytes] | None = None
        self.stats = stats
        self.stream_handler = stream_handler
        self.target_dir = target_dir
        self.temp_dir = temp_dir
        self.backup_xtrabackup_info: Dict[str, str] | None = None
        self.backup_tool_version = backup_tool_version
        # LSN progress for xtrabackup --prepare.
        # * prepare_last_lsn is the redo stop LSN, read from the xtrabackup_checkpoints
        #   field "last_lsn". This is the upper bound of the captured redo stream and
        #   the LSN that "Doing recovery: scanned up to ..." actually advances to
        #   during --prepare. The checkpoints file also carries a "to_lsn" — that's
        #   the last checkpoint LSN at backup start, which is ≤ last_lsn; using it as
        #   the denominator would make the bar hit 100% before the tail of the redo
        #   stream has been applied.
        # * prepare_scan_start_lsn is the first LSN observed in a
        #   "Doing recovery: scanned up to log sequence number N" line. We don't use
        #   from_lsn from the checkpoints here because it's 0 for every full backup
        #   (it's the base-LSN for incrementals), which would pin pct at ~99% for
        #   almost the entire prepare run — the actual redo replayed during --prepare
        #   spans roughly the backup's duration, not the full history of the server.
        # * prepare_current_lsn is the latest scan-line LSN seen.
        self.prepare_last_lsn: Optional[int] = None
        self.prepare_scan_start_lsn: Optional[int] = None
        self.prepare_current_lsn: Optional[int] = None
        # Last integer pct fired to prepare_progress_callback, so we don't fire
        # the callback repeatedly when successive scan lines advance the LSN but
        # truncate to the same pct. Reset on each prepare_backup call.
        self._last_emitted_prepare_pct: Optional[int] = None
        #: Fired when the xtrabackup --prepare subprocess is about to start (with
        #: pct=None) and again whenever the derived pct changes during the run.
        #: Called on the subprocess reader thread — the coordinator handles
        #: synchronisation.
        self.prepare_progress_callback: Optional[Callable[..., None]] = None

    def prepare_backup(
        self, incremental: bool = False, apply_log_only: bool = False, checkpoints_file_content: str | None = None
    ):
        # Write encryption key to file to avoid having it on command line. NamedTemporaryFile has mode 0600

        # Reset any progress state from a previous invocation of this object so stale
        # values don't leak across prepare calls (e.g. required-backup iteration).
        # last_lsn is a required key of xtrabackup_checkpoints; if the caller has no
        # checkpoints content (legacy backups taken before myhoard captured it) we
        # leave the bounds unset and pct stays None through the run.
        self.prepare_last_lsn = None
        self.prepare_scan_start_lsn = None
        self.prepare_current_lsn = None
        self._last_emitted_prepare_pct = None
        if checkpoints_file_content:
            checkpoints = parse_xtrabackup_info(checkpoints_file_content)
            self.prepare_last_lsn = int(checkpoints["last_lsn"])

        incremental_dir = None
        try:
            if incremental:
                incremental_dir = tempfile.mkdtemp(dir=self.temp_dir, prefix="myhoard_inc_")

            with tempfile.NamedTemporaryFile() as encryption_key_file:
                encryption_key_file.write(base64.b64encode(self.encryption_key))
                encryption_key_file.flush()

                cpu_count = multiprocessing.cpu_count()
                extract_dir = self.target_dir if not incremental_dir else incremental_dir
                command_line = [
                    "xbstream",
                    # TODO: Check if it made sense to restore directly to MySQL data directory so that
                    # we could skip the move phase. It's not clear if move does anything worthwhile
                    # except skips a few extra files, which could instead be deleted explicitly
                    "--directory",
                    extract_dir,
                    "--extract",
                    "--decompress",
                    "--decompress-threads",
                    str(max(cpu_count - 1, 1)),
                    "--decrypt",
                    self.encryption_algorithm,
                    "--encrypt-key-file",
                    encryption_key_file.name,
                    "--encrypt-threads",
                    str(max(cpu_count - 1, 1)),
                    "--parallel",
                    str(max(cpu_count - 1, 1)),
                    "--verbose",
                ]

                with self.stats.timing_manager(
                    "myhoard.basebackup_restore.xbstream_extract", tags={"incremental": incremental}
                ):
                    with subprocess.Popen(
                        command_line, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    ) as xbstream:
                        self.proc = xbstream
                        self._process_xbstream_input_output()

                # TODO: Get some execution time numbers with non-trivial data sets for --prepare
                # and --move-back commands and add progress monitoring if necessary (and feasible)
                command_line = [
                    self.get_xtrabackup_cmd(),
                    # defaults file must be given with --defaults-file=foo syntax, space here does not work
                    f"--defaults-file={self.mysql_config_file_name}",
                    "--no-version-check",
                    "--prepare",
                    "--target-dir",
                    self.target_dir,
                ]

                if apply_log_only:
                    # This is needed to prepare all the backups preceding the one to be restored in case of incremental
                    command_line.append("--apply-log-only")
                if incremental_dir:
                    command_line.extend(["--incremental-dir", incremental_dir])

                if checkpoints_file_content:
                    checkpoints_file_dir = incremental_dir if incremental_dir else self.target_dir
                    with open(os.path.join(checkpoints_file_dir, CHECKPOINT_FILENAME), "w") as checkpoints_file:
                        checkpoints_file.write(checkpoints_file_content)

                # --use-free-memory-pct introduced in 8.0.30, but it doesn't work in 8.0.30 and leads to PBX crash
                if self.free_memory_percentage is not None and get_xtrabackup_version() >= (8, 0, 32):
                    command_line.insert(2, f"--use-free-memory-pct={self.free_memory_percentage}")
                self._emit_prepare_progress(pct=None)
                with self.stats.timing_manager("myhoard.basebackup_restore.xtrabackup_prepare"):
                    with subprocess.Popen(
                        command_line, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    ) as prepare:
                        self.proc = prepare
                        self._process_prepare_input_output()
        finally:
            if incremental_dir:
                shutil.rmtree(incremental_dir)

    def restore_backup(self):
        # As of Percona XtraBackup 8.0.5 the backup contains binlog.index and one binlog that --move-back
        # tries to restore to appropriate location, presumable with the intent of making MySQL patch gtid_executed
        # table based on what's in the log. We already have logic for patching the table and we don't need
        # this logic from XtraBackup. The logic is also flawed as it tries to restore to location that existed
        # on the source server, which may not be valid for destination server. Just delete the files to disable
        # that restoration logic.

        binlog_index = os.path.join(self.target_dir, "binlog.index")
        if os.path.exists(binlog_index):
            with open(binlog_index) as f:
                binlogs = f.read().split("\n")
            binlogs = [binlog.rsplit("/", 1)[-1] for binlog in binlogs if binlog.strip()]
            self.log.info("Deleting redundant binlog index %r and binlogs %r before move", binlog_index, binlogs)
            os.remove(binlog_index)
            for binlog_name in binlogs:
                binlog_name = os.path.join(self.target_dir, binlog_name)
                if os.path.exists(binlog_name):
                    os.remove(binlog_name)

        command_line = [
            self.get_xtrabackup_cmd(),
            # defaults file must be given with --defaults-file=foo syntax, space here does not work
            f"--defaults-file={self.mysql_config_file_name}",
            "--move-back",
            "--no-version-check",
            "--target-dir",
            self.target_dir,
        ]
        with self.stats.timing_manager("myhoard.basebackup_restore.xtrabackup_move"):
            with subprocess.Popen(command_line, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as move_back:
                self.proc = move_back
                self._process_move_input_output()

        self.data_directory_size_end = self._get_directory_size(self.mysql_data_directory, cleanup=True)

    def get_xtrabackup_cmd(self) -> str:
        backup_xtrabackup_version = parse_version(self.backup_tool_version) if self.backup_tool_version else None

        xtrabackup_cmd = "xtrabackup"
        if backup_xtrabackup_version:
            for bin_info in find_extra_xtrabackup_executables():
                if bin_info.version[:3] == backup_xtrabackup_version[:3]:
                    xtrabackup_cmd = str(bin_info.path)
                    break
        self.log.info("get_xtrabackup_cmd: using %s for version %s", xtrabackup_cmd, backup_xtrabackup_version)
        return xtrabackup_cmd

    def _get_directory_size(self, directory_name, cleanup=False):
        # auto.cnf shouldn't be present to begin with but make extra sure so that we get new server UUID
        cleanup_names = {"auto.cnf", "xtrabackup*"}

        total_size = 0
        for dirpath, _dirnames, filenames in os.walk(directory_name):
            for filename in filenames:
                full_file_name = os.path.join(dirpath, filename)
                if cleanup:
                    if any(fnmatch.fnmatch(filename, pattern) for pattern in cleanup_names):
                        os.remove(full_file_name)
                        continue

                try:
                    total_size += os.path.getsize(full_file_name)
                except OSError as ex:
                    self.log.warning("Failed to get size for %r: %r", full_file_name, ex)

        return total_size

    def _process_output_loop(self, operation, loop_fn, output_fn):
        assert self.proc is not None
        assert self.proc.stderr is not None
        assert self.proc.stdout is not None
        set_stream_nonblocking(self.proc.stderr)
        set_stream_nonblocking(self.proc.stdout)

        pending_outputs = {"stderr": b"", "stdout": b""}
        exit_code = None
        try:
            while exit_code is None:
                rlist, _, _ = select.select([self.proc.stderr, self.proc.stdout], [], [], 0.2)
                for fd in rlist:
                    stream_name = "stderr" if fd == self.proc.stderr else "stdout"
                    content = fd.read()
                    if content:
                        if pending_outputs[stream_name]:
                            content = pending_outputs[stream_name] + content
                        lines = content.splitlines(keepends=True)
                        for line in lines[:-1]:
                            output_fn(line, stream_name)

                        # Don't process partial lines
                        if b"\n" not in lines[-1]:
                            pending_outputs[stream_name] = lines[-1]
                        else:
                            output_fn(lines[-1], stream_name)
                            pending_outputs[stream_name] = b""
                exit_code = self.proc.poll()
                if loop_fn:
                    loop_fn()

            pending_outputs["stderr"] += self.proc.stderr.read() or b""
            pending_outputs["stdout"] += self.proc.stdout.read() or b""
            if exit_code == 0:
                for stream_name, pending_output in pending_outputs.items():
                    if pending_output:
                        for line in pending_output.splitlines():
                            output_fn(line, stream_name)
        except Exception as ex:
            pending_outputs["stderr"] += self.proc.stderr.read() or b""
            self.log.error("Error %r occurred in %s operation, output: %r", ex, operation, pending_outputs["stderr"])
            raise ex
        finally:
            # If the process isn't dead yet make sure it is now
            if exit_code is None:
                with suppress(Exception):
                    self.proc.kill()
            with suppress(Exception):
                self.proc.stdout.close()
            with suppress(Exception):
                self.proc.stderr.close()

        if exit_code != 0:
            self.log.error("Operation %s failed with exit code %s: %r", operation, exit_code, pending_outputs["stderr"])
            raise Exception(f"Operation {operation} failed with code {exit_code}")

    def _process_move_input_output(self):
        assert self.proc is not None
        increase_pipe_capacity(self.proc.stdout, self.proc.stderr)

        self._process_output_loop("xtrabackup-move", None, self._process_move_output_line)

    def _process_prepare_input_output(self):
        assert self.proc is not None
        increase_pipe_capacity(self.proc.stdout, self.proc.stderr)

        self._process_output_loop("xtrabackup-prepare", None, self._process_prepare_output_line)

        if self.prepared_lsn is None:
            raise Exception("Could not read prepared lsn from xtrabackup output")

    def _process_xbstream_input_output(self):
        assert self.proc is not None
        increase_pipe_capacity(self.proc.stdin, self.proc.stdout, self.proc.stderr)

        sender_thread = InputSenderThread(stats=self.stats, stream_handler=self.stream_handler, stream=self.proc.stdin)
        sender_thread.start()

        def loop_fn():
            if sender_thread.exception:
                raise sender_thread.exception  # pylint: disable=raising-bad-type

        try:
            self._process_output_loop("xbstream-extract", loop_fn, self._process_xbstream_output_line)
        finally:
            with suppress(Exception):
                if self.proc.stdin:
                    self.proc.stdin.close()
            self.log.info("Joining output reader thread...")
            # We've closed stdin so the thread is bound to exit without any other calls
            sender_thread.join()
            self.log.info("Thread joined")

        # Sender thread might have encountered an exception after xbstream exited if it hadn't
        # yet finished sending data to it
        if sender_thread.exception:
            raise sender_thread.exception  # pylint: disable=raising-bad-type

    @staticmethod
    def decode_output_line(line):
        line = line.rstrip()
        if not line:
            return line

        try:
            return line.decode("utf-8", "strict")
        except UnicodeDecodeError:
            return line.decode("iso-8859-1")

    def _process_move_output_line(self, line: str, _stream_name: str) -> None:
        line = self.decode_output_line(line)
        if not line:
            return

        self._raise_if_no_space_left(line, "xtrabackup-move")

        self.log.info("xtrabackup-move: %r", line)

    @property
    def prepare_progress_pct(self) -> Optional[int]:
        """xtrabackup --prepare progress as a 0..100 int, derived on demand.

        None when last_lsn isn't known (legacy backup without xtrabackup_checkpoints)
        or before the first scan line has been seen.

        The lower bound is the first observed scan-line LSN, not from_lsn: on a
        full backup from_lsn is 0 (it's the base-LSN for incrementals), so using
        it would produce a bar that barely moves and then jumps from 99 to 100.
        """
        if self.prepare_last_lsn is None or self.prepare_scan_start_lsn is None or self.prepare_current_lsn is None:
            return None
        span = self.prepare_last_lsn - self.prepare_scan_start_lsn
        if span <= 0:
            return 100
        return max(0, min(100, int((self.prepare_current_lsn - self.prepare_scan_start_lsn) * 100.0 / span)))

    def _process_prepare_output_line(self, line: str, _stream_name: str) -> None:
        line = self.decode_output_line(line)
        if not line:
            return

        self._raise_if_no_space_left(line, "xtrabackup-prepare")

        if self._advance_prepare_lsn(line):
            self._emit_prepare_progress(pct=self.prepare_progress_pct)

        if not self._process_prepare_completed_line(line):
            self.log.info("xtrabackup-prepare: %r", line)

    def _emit_prepare_progress(self, *, pct: Optional[int]) -> None:
        """Fire prepare_progress_callback iff pct differs from the last emission.

        Scan-line LSNs advance much more often than the derived integer pct: an
        LSN bump of a few KB produces the same pct until the span crosses a 1 %
        boundary. Without this guard we'd take the coordinator's state lock for
        every scan line even though update_state would be a no-op, and any
        non-coordinator callback user would see duplicates.

        None is special-cased as "the prepare is starting" and always fires —
        the coordinator relies on it to flip into Phase.preparing_backup.
        """
        if self.prepare_progress_callback is None:
            return
        if pct is not None and pct == self._last_emitted_prepare_pct:
            return
        self._last_emitted_prepare_pct = pct
        self.prepare_progress_callback(pct=pct)

    def _process_prepare_completed_line(self, line):
        match = self.prepare_completed_re.search(line)
        if match:
            self.prepared_lsn = int(match.group(1))
            self.log.info("Restored backup prepared, lsn %s", self.prepared_lsn)
            # Pin the bar at 100 on the final shutdown line even if intermediate scan
            # lines didn't reach last_lsn (tiny redo log, already-prepared backup, or
            # int() truncation on a small range). Seed scan_start too if no scan
            # line ever landed — collapsing the window to zero yields pct=100 from
            # the property (handled by the span<=0 branch).
            if self.prepare_last_lsn is not None and (
                self.prepare_current_lsn is None or self.prepare_current_lsn < self.prepare_last_lsn
            ):
                self.prepare_current_lsn = self.prepare_last_lsn
                if self.prepare_scan_start_lsn is None:
                    self.prepare_scan_start_lsn = self.prepare_last_lsn
                self._emit_prepare_progress(pct=self.prepare_progress_pct)
        return match

    def _advance_prepare_lsn(self, line: str) -> bool:
        """Update prepare_current_lsn from a scan line.

        Returns True only when the LSN advanced *and* a pct can be derived. Out-of-
        order scan lines with a smaller LSN are ignored. Lines seen before last_lsn
        is known also return False (no denominator yet).

        The first scan line also seeds prepare_scan_start_lsn; that's the lower
        bound of the pct window, so until it's set the property returns None.
        """
        if self.prepare_last_lsn is None:
            return False
        match = self.prepare_lsn_re.search(line)
        if not match:
            return False
        lsn = int(match.group(1))
        if self.prepare_current_lsn is not None and lsn <= self.prepare_current_lsn:
            return False
        self.prepare_current_lsn = lsn
        if self.prepare_scan_start_lsn is None:
            self.prepare_scan_start_lsn = lsn
        return True

    def _process_xbstream_output_line(self, line: str, _stream_name: str) -> None:
        line = self.decode_output_line(line)
        if not line:
            return

        self._raise_if_no_space_left(line, "xbstream-extract")

        if not self._process_xbstream_output_line_new_file(line):
            self.log.info("xbstream: %r", line)

    def _process_xbstream_output_line_new_file(self, line):
        if not line.endswith(".xbcrypt"):
            return False

        self.current_file = line
        self.number_of_files += 1
        self.log.info("Started processing file %r", self.current_file)
        return True

    def _raise_if_no_space_left(self, line: str, operation: str) -> None:
        if self.NO_SPACE_LEFT_ON_DEVICE in line:
            self.log.error("%s: %r", operation, line)
            raise DiskFullError(f"No space left on device. Cannot complete {operation}!")


class InputSenderThread(threading.Thread):
    def __init__(self, *, stats, stream_handler, stream):
        super().__init__()
        self.exception = None
        self.stats = stats
        self.stream_handler = stream_handler
        self.stream = stream

    def run(self):
        try:
            self.stream_handler(self.stream)
            # Must ensure the stream is closed since xbstream does not exit before it reaches EOF
            with suppress(Exception):
                self.stream.close()
        except Exception as ex:  # pylint: disable=broad-except
            logging.getLogger(self.__class__.__name__).exception("Failure while sending restore input")
            self.stats.unexpected_exception(ex=ex, where="BasebackupRestoreInputSenderThread")
            self.exception = ex
