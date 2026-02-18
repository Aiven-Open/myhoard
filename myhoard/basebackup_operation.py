# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from contextlib import suppress
from myhoard.errors import BlockMismatchError, XtraBackupError
from myhoard.util import CHECKPOINT_FILENAME, get_mysql_version, mysql_cursor, parse_xtrabackup_info
from packaging.version import Version
from rohmu.util import increase_pipe_capacity, set_stream_nonblocking
from typing import Any, Dict, Optional

import base64
import logging
import os
import re
import select
import shutil
import subprocess
import tempfile
import threading

# Number of seconds to allow for database operations to complete
# while running the OPTIMIZE TABLE mitigation
CURSOR_TIMEOUT_DURING_OPTIMIZE: int = 120


class AbortRequested(Exception):
    """The base backup operation was aborted by request."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"reason: {reason}")
        self.reason = reason


class BasebackupOperation:
    """Creates a new basebackup. Provides callback for getting progress info, extracts
    details of the created backup (like the binlog position up to which the backup has
    data) and executes callback that can read the actual backup data. This class does
    not handle persistence of the backup or any metadata. Note that the stream handler
    callback is executed on another thread so that the main thread can manage backup
    state information while the data is being persisted."""

    current_file_re = re.compile(r" Compressing, encrypting and streaming\s?(.*?)( to <STDOUT>)?( up to position \d+)?$")
    binlog_info_re = re.compile(
        r"filename '(?P<fn>.+)', position '(?P<pos>\d+)'(, GTID of the last change '(?P<gtid>.+)')?$"
    )
    lsn_re = re.compile(r" Transaction log of lsn \((\d+)\) to \((\d+)\) was copied.$")
    progress_file_re = re.compile(r"(ib_buffer_pool$)|(ibdata\d+$)|(.*\.CSM$)|(.*\.CSV$)|(.*\.ibd$)|(.*\.sdi$)|(undo_\d+$)")

    def __init__(
        self,
        *,
        copy_threads=1,
        compress_threads=1,
        encrypt_threads=1,
        encryption_algorithm,
        encryption_key,
        mysql_client_params,
        mysql_config_file_name,
        mysql_data_directory,
        optimize_tables_before_backup=False,
        progress_callback=None,
        register_redo_log_consumer=False,
        stats,
        stream_handler,
        temp_dir,
        incremental_since_checkpoint: str | None = None,
    ):
        self.abort_reason = None
        self.binlog_info: Dict[str, Any] | None = None
        self.checkpoints_file_content: str | None = None
        self.copy_threads = copy_threads
        self.compress_threads = compress_threads
        self.current_file = None
        self.data_directory_filtered_size = None
        self.data_directory_size_end: Optional[int] = None
        self.data_directory_size_start: Optional[int] = None
        self.encrypt_threads = encrypt_threads
        self.encryption_algorithm = encryption_algorithm
        self.encryption_key = encryption_key
        self.has_block_mismatch = False
        self.log = logging.getLogger(self.__class__.__name__)
        self.incremental_since_checkpoint = incremental_since_checkpoint
        self.prev_checkpoint_dir = None
        self.lsn_dir: str | None = None
        self.lsn_info = None
        self.mysql_client_params = mysql_client_params
        with open(mysql_config_file_name, "r") as config:
            self.mysql_config = config.read()
        self.mysql_data_directory = mysql_data_directory
        self.number_of_files = 0
        self.optimize_tables_before_backup = optimize_tables_before_backup
        self.proc = None
        self.processed_original_bytes = 0
        self.progress_callback = progress_callback
        self.register_redo_log_consumer = register_redo_log_consumer
        self.stats = stats
        self.stream_handler = stream_handler
        self.temp_dir: Optional[str] = None
        self.temp_dir_base = temp_dir
        self.tool_version: str | None = None

    def abort(self, reason):
        """Aborts ongoing backup generation"""
        self.abort_reason = reason

    def create_backup(self):
        self.abort_reason = None
        self.data_directory_size_start, self.data_directory_filtered_size = self._get_data_directory_size()
        self._update_progress()
        if self.optimize_tables_before_backup:
            self._optimize_tables()

        # Write encryption key to file to avoid having it on command line. NamedTemporaryFile has mode 0600
        with tempfile.NamedTemporaryFile(
            dir=self.temp_dir_base, delete=True, prefix="encrkey", suffix="bin"
        ) as encryption_key_file:
            encryption_key_file.write(base64.b64encode(self.encryption_key))
            encryption_key_file.flush()

            # Create new configuration file that has original MySQL config plus user and password
            # for connecting to it to avoid having those on command line
            with tempfile.NamedTemporaryFile(
                dir=self.temp_dir_base, delete=True, prefix="mysql", mode="w", suffix="conf"
            ) as mysql_config_file:
                mysql_config_file.write(self.mysql_config)
                client_params_str = "\n".join(f"{k}={v}" for k, v in self.mysql_client_params.items())
                mysql_config_file.write(f"\n[client]\n{client_params_str}\n")
                mysql_config_file.flush()

                self.temp_dir = tempfile.mkdtemp(dir=self.temp_dir_base, prefix="xtrabackup")
                self.lsn_dir = tempfile.mkdtemp(dir=self.temp_dir_base, prefix="xtrabackupmeta")
                command_line = [
                    "xtrabackup",
                    # defaults file must be given with --defaults-file=foo syntax, space here does not work
                    f"--defaults-file={mysql_config_file.name}",
                    "--backup",
                    "--compress",
                    f"--compress-threads={self.compress_threads}",
                    "--encrypt",
                    self.encryption_algorithm,
                    f"--encrypt-threads={self.encrypt_threads}",
                    "--encrypt-key-file",
                    encryption_key_file.name,
                    "--no-version-check",
                    f"--parallel={self.copy_threads}",
                    "--stream=xbstream",
                    "--target-dir",
                    self.temp_dir,
                    "--extra-lsndir",
                    self.lsn_dir,
                ]

                if self.register_redo_log_consumer:
                    command_line.append("--register-redo-log-consumer")

                if self.incremental_since_checkpoint:
                    self.prev_checkpoint_dir = tempfile.mkdtemp(dir=self.temp_dir_base, prefix="xtrabackupcheckpoint")
                    self._write_checkpoints_file(self.prev_checkpoint_dir)
                    command_line.extend(["--incremental-basedir", self.prev_checkpoint_dir])

                with self.stats.timing_manager(
                    "myhoard.basebackup.xtrabackup_backup",
                    tags={"incremental": self.is_incremental()},
                ):
                    with subprocess.Popen(
                        command_line, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    ) as xtrabackup:
                        self.proc = xtrabackup
                        self._process_input_output()

        self.data_directory_size_end, self.data_directory_filtered_size = self._get_data_directory_size()
        self._update_progress(estimated_progress=100)

    def is_incremental(self) -> bool:
        return self.incremental_since_checkpoint is not None

    def _optimize_tables(self) -> None:
        params = dict(self.mysql_client_params)
        params["timeout"] = CURSOR_TIMEOUT_DURING_OPTIMIZE
        with mysql_cursor(**params) as cursor:
            version = get_mysql_version(cursor)
            if Version(version) < Version("8.0.29"):
                return

            # allow OPTIMIZE TABLE to run on tables without primary keys
            cursor.execute("SET @@SESSION.sql_require_primary_key = 0;")

            def unescape_to_utf8(escaped: str) -> Optional[str]:
                ret = re.sub(r"@([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), escaped)
                ret = re.sub(
                    r"@([0-9a-fA-F])([a-zA-Z])", lambda m: chr(ord(m.group(2)) + 121 + 20 * int(m.group(1), 16)), ret
                )
                if "`" in ret or "\\" in ret:
                    # bail out so we don't unescape ourselves below
                    return None
                return ret

            database_and_tables = []
            cursor.execute("SELECT NAME FROM INFORMATION_SCHEMA.INNODB_TABLES WHERE TOTAL_ROW_VERSIONS > 0")
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                db_and_table = row["NAME"].split("/")
                database = unescape_to_utf8(db_and_table[0])
                table = unescape_to_utf8(db_and_table[1])
                if database is None or table is None:
                    self.log.warning("Could not decode database/table name of '%s'", row["NAME"])
                    continue
                database_and_tables.append((database, table))

            for database, table in database_and_tables:
                self.stats.increase(metric="myhoard.basebackup.optimize_table", tags={"incremental": self.is_incremental()})
                self.log.info("Optimizing table %r.%r", database, table)
                # sending it as parameters doesn't work
                cursor.execute(f"OPTIMIZE TABLE `{database}`.`{table}`")
                cursor.execute("COMMIT")

    def _get_data_directory_size(self):
        total_filtered_size = 0
        total_size = 0
        for dirpath, _dirnames, filenames in os.walk(self.mysql_data_directory):
            for filename in filenames:
                full_file_name = os.path.join(dirpath, filename)
                try:
                    file_size = os.path.getsize(full_file_name)
                    total_size += file_size
                    if self.progress_file_re.search(filename):
                        total_filtered_size += file_size
                except OSError as ex:
                    self.log.warning("Failed to get size for %r: %r", full_file_name, ex)

        return total_size, total_filtered_size

    def _process_input_output(self):
        assert self.proc is not None
        assert self.proc.stdout is not None
        assert self.proc.stderr is not None
        increase_pipe_capacity(self.proc.stdout, self.proc.stderr)
        set_stream_nonblocking(self.proc.stderr)

        reader_thread = OutputReaderThread(stats=self.stats, stream_handler=self.stream_handler, stream=self.proc.stdout)
        reader_thread.start()

        pending_output = b""
        exit_code = None
        try:
            while exit_code is None:
                rlist, _, _ = select.select([self.proc.stderr], [], [], 0.2)
                for fd in rlist:
                    content = fd.read()
                    if content:
                        if pending_output:
                            content = pending_output + content
                        lines = content.splitlines(keepends=True)
                        for line in lines[:-1]:
                            self._process_output_line(line)

                        # Don't process partial lines
                        if b"\n" not in lines[-1]:
                            pending_output = lines[-1]
                        else:
                            self._process_output_line(lines[-1])
                            pending_output = b""
                exit_code = self.proc.poll()
                if reader_thread.exception:
                    raise reader_thread.exception  # pylint: disable=raising-bad-type
                if self.abort_reason:
                    raise AbortRequested(self.abort_reason)

            pending_output += self.proc.stderr.read() or b""
            if exit_code is not None and pending_output:
                for line in pending_output.splitlines():
                    self._process_output_line(line)
            # Process has exited but reader thread might still be processing stdout. Wait for
            # the thread to exit before proceeding
            if exit_code is not None:
                self.log.info("Process has exited, joining reader thread")
                reader_thread.join()
                reader_thread = None

                if exit_code == 0:
                    self._read_checkpoints_file()
                    self._process_xtrabackup_info()

        except AbortRequested as ex:
            self.log.info("Abort requested: %s", ex.reason)
            raise
        except Exception as ex:
            pending_output += self.proc.stderr.read() or b""
            self.log.error("Error %r occurred while creating backup, output: %r", ex, pending_output)
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
            self.log.info("Joining output reader thread...")
            # We've closed stdout so the thread is bound to exit without any other calls
            if reader_thread:
                reader_thread.join()
            self.log.info("Thread joined")
            for d in [self.lsn_dir, self.temp_dir, self.prev_checkpoint_dir]:
                if d:
                    shutil.rmtree(d)
            self.proc = None

        if exit_code != 0:
            self.log.error("xtrabackup exited with non-zero exit code %s: %r", exit_code, pending_output)
            if self.has_block_mismatch:
                raise BlockMismatchError(f"xtrabackup failed with code {exit_code} due log block mismatch.")
            raise XtraBackupError(f"xtrabackup failed with code {exit_code}")

        # Reader thread might have encountered an exception after xtrabackup exited if it hadn't
        # yet finished storing data to backup location
        if reader_thread and reader_thread.exception:
            raise reader_thread.exception  # pylint: disable=raising-bad-type

    def _process_output_line(self, line):
        line = line.rstrip()
        if not line:
            return

        try:
            line = line.decode("utf-8", "strict")
        except UnicodeDecodeError:
            line = line.decode("iso-8859-1")

        if (
            not self._process_output_line_new_file(line)
            and not self._process_output_line_file_finished(line)
            and not self._process_output_line_lsn_info(line)
        ):
            if any(key in line for key in ["[ERROR]", " Failed ", " failed ", " Invalid "]):
                if "log block numbers mismatch" in line or "expected log block no" in line:
                    self.has_block_mismatch = True

                self.log.error("xtrabackup: %r", line)
            else:
                self.log.info("xtrabackup: %r", line)

    def _process_output_line_new_file(self, line):
        match = self.current_file_re.search(line)
        if match and ("Done:" not in line):
            self.current_file = match.group(1)
            if self.current_file != "<STDOUT>":
                self.log.info("Started processing file %r", self.current_file)
            return True
        return False

    def _process_output_line_file_finished(self, line):
        if not (line.endswith(" ...done") or ("Done:" in line)):
            return False

        if not self.current_file:
            self.log.warning("Processing of file finished but no file was being tracked: %r", line)
        elif self.current_file != "<STDOUT>":
            self.number_of_files += 1
            full_name = os.path.join(self.mysql_data_directory, self.current_file)
            try:
                file_size = os.path.getsize(full_name)
                self.processed_original_bytes += file_size
                self._update_progress(last_file_name=self.current_file, last_file_size=file_size)
                self.log.info("Processing %r finished, file size %s bytes", self.current_file, file_size)
            except OSError as ex:
                self.log.warning("Failed to get size for %r to update progress: %r", full_name, ex)

        self.current_file = None
        return True

    def _read_checkpoints_file(self) -> None:
        assert self.lsn_dir

        with open(os.path.join(self.lsn_dir, CHECKPOINT_FILENAME)) as checkpoints_file:
            self.checkpoints_file_content = checkpoints_file.read()

    def _write_checkpoints_file(self, checkpoints_dir: str) -> None:
        assert self.incremental_since_checkpoint

        with open(os.path.join(checkpoints_dir, CHECKPOINT_FILENAME), "w") as checkpoint_file:
            checkpoint_file.write(self.incremental_since_checkpoint)

    def _process_xtrabackup_info(self) -> None:
        assert self.lsn_dir

        xtrabackup_info_file_path = os.path.join(self.lsn_dir, "xtrabackup_info")
        with open(xtrabackup_info_file_path) as fh:
            backup_xtrabackup_info = parse_xtrabackup_info(fh.read())

        binlog_pos_str = backup_xtrabackup_info.get("binlog_pos")
        if binlog_pos_str:
            if match := self.binlog_info_re.match(binlog_pos_str):
                self.binlog_info = {
                    "file_name": match.group("fn"),
                    "file_position": int(match.group("pos")),
                    "gtid": match.group("gtid"),
                }
                self.log.info("binlog info: %r", self.binlog_info)
            else:
                self.log.error("Can't parse binlog info from `xtrabackup_info` file")
        else:
            self.log.warning("binlog info wasn't found in `xtrabackup_info` file")

        self.tool_version = backup_xtrabackup_info.get("tool_version")

    def _process_output_line_lsn_info(self, line):
        match = self.lsn_re.search(line)
        if match:
            self.lsn_info = {
                "start": int(match.group(1)),
                "end": int(match.group(2)),
            }
            self.log.info("Transaction log lsn info: %r", self.lsn_info)
        return match

    def _update_progress(self, *, last_file_name=None, last_file_size=None, estimated_progress=None):
        estimated_total_bytes = self.data_directory_filtered_size or 0

        if estimated_progress is None:
            estimated_progress = 0
            if estimated_total_bytes > 0:
                estimated_progress = min(self.processed_original_bytes / estimated_total_bytes * 100, 100)

        self.log.info(
            "Processed %s bytes of estimated %s total bytes, progress at %.2f%%",
            self.processed_original_bytes,
            estimated_total_bytes,
            estimated_progress,
        )
        self.stats.gauge_float(
            "myhoard.basebackup.estimated_progress", estimated_progress, tags={"incremental": self.is_incremental()}
        )

        if self.progress_callback:
            self.progress_callback(
                estimated_progress=estimated_progress,
                estimated_total_bytes=estimated_total_bytes,
                last_file_name=last_file_name,
                last_file_size=last_file_size,
                processed_original_bytes=self.processed_original_bytes,
            )


class OutputReaderThread(threading.Thread):
    def __init__(self, *, stats, stream_handler, stream):
        super().__init__()
        self.exception = None
        self.stats = stats
        self.stream_handler = stream_handler
        self.stream = stream

    def run(self):
        try:
            self.stream_handler(self.stream)
        except Exception as ex:  # pylint: disable=broad-except
            logging.getLogger(self.__class__.__name__).exception("Failure while processing backup output")
            self.stats.increase("myhoard.basebackup_read_error", tags={"ex": ex.__class__.__name__})
            self.exception = ex
