# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import base64
import logging
import os
import re
import select
import shutil
import subprocess
import tempfile
import threading
from contextlib import suppress

from pghoard.common import increase_pipe_capacity, set_stream_nonblocking

from myhoard.errors import XtraBackupError


class BasebackupOperation:
    """Creates a new basebackup. Provides callback for getting progress info, extracts
    details of the created backup (like the binlog position up to which the backup has
    data) and executes callback that can read the actual backup data. This class does
    not handle persistence of the backup or any metadata. Note that the stream handler
    callback is executed on another thread so that the main thread can manage backup
    state information while the data is being persisted."""

    current_file_re = re.compile(r" Compressing, encrypting and streaming (.*?)( to <STDOUT>)?( up to position \d+)?$")
    binlog_info_re = re.compile(
        r"MySQL binlog position: filename '(?P<fn>.+)', position '(?P<pos>\d+)'(, GTID of the last change '(?P<gtid>.+)')?$"
    )
    lsn_re = re.compile(r" Transaction log of lsn \((\d+)\) to \((\d+)\) was copied.$")
    progress_file_re = re.compile(r"(ib_buffer_pool$)|(ibdata\d+$)|(.*\.CSM$)|(.*\.CSV$)|(.*\.ibd$)|(.*\.sdi$)|(undo_\d+$)")

    def __init__(
        self,
        *,
        encryption_algorithm,
        encryption_key,
        mysql_client_params,
        mysql_config_file_name,
        mysql_data_directory,
        progress_callback=None,
        stats,
        stream_handler,
        temp_dir,
    ):
        self.abort_reason = None
        self.binlog_info = None
        self.current_file = None
        self.data_directory_filtered_size = None
        self.data_directory_size_end = None
        self.data_directory_size_start = None
        self.encryption_algorithm = encryption_algorithm
        self.encryption_key = encryption_key
        self.log = logging.getLogger(self.__class__.__name__)
        self.lsn_info = None
        self.mysql_client_params = mysql_client_params
        with open(mysql_config_file_name, "r") as config:
            self.mysql_config = config.read()
        self.mysql_data_directory = mysql_data_directory
        self.number_of_files = 0
        self.proc = None
        self.processed_original_bytes = 0
        self.progress_callback = progress_callback
        self.stats = stats
        self.stream_handler = stream_handler
        self.temp_dir = None
        self.temp_dir_base = temp_dir

    def abort(self, reason):
        """Aborts ongoing backup generation"""
        self.abort_reason = reason

    def create_backup(self):
        self.abort_reason = None
        self.data_directory_size_start, self.data_directory_filtered_size = self._get_data_directory_size()
        self._update_progress()

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
                # yapf: disable
                command_line = [
                    "xtrabackup",
                    # defaults file must be given with --defaults-file=foo syntax, space here does not work
                    f"--defaults-file={mysql_config_file.name}",
                    "--backup",
                    "--compress",
                    "--encrypt", self.encryption_algorithm,
                    "--encrypt-key-file", encryption_key_file.name,
                    "--no-version-check",
                    "--stream", "xbstream",
                    "--target-dir", self.temp_dir,
                ]

                with self.stats.timing_manager("myhoard.basebackup.xtrabackup_backup"):
                    with subprocess.Popen(
                        command_line, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    ) as xtrabackup:
                        self.proc = xtrabackup
                        self._process_input_output()

        self.data_directory_size_end, self.data_directory_filtered_size = self._get_data_directory_size()
        self._update_progress()

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
                    raise Exception(f"Operation abort requested: {self.abort_reason}")

            pending_output += self.proc.stderr.read() or b""
            if exit_code == 0 and pending_output:
                for line in pending_output.splitlines():
                    self._process_output_line(line)
            # Process has exited but reader thread might still be processing stdout. Wait for
            # the thread to exit before proceeding
            if exit_code == 0:
                self.log.info("Process has exited, joining reader thread")
                reader_thread.join()
                reader_thread = None
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
            shutil.rmtree(self.temp_dir)
            self.proc = None

        if exit_code != 0:
            self.log.error("xtrabackup exited with non-zero exit code %s: %r", exit_code, pending_output)
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
            not self._process_output_line_new_file(line) and
            not self._process_output_line_file_finished(line) and
            not self._process_output_line_binlog_info(line) and
            not self._process_output_line_lsn_info(line)
        ):
            if any(key in line for key in {"[ERROR]", " Failed ", " failed ", " Invalid "}):
                self.log.error("xtrabackup: %r", line)
            else:
                self.log.info("xtrabackup: %r", line)

    def _process_output_line_new_file(self, line):
        match = self.current_file_re.search(line)
        if match:
            self.current_file = match.group(1)
            if self.current_file != "<STDOUT>":
                self.log.info("Started processing file %r", self.current_file)
        return match

    def _process_output_line_file_finished(self, line):
        if not line.endswith(" ...done"):
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

    def _process_output_line_binlog_info(self, line):
        match = self.binlog_info_re.search(line)
        if match:
            self.binlog_info = {
                "file_name": match.group("fn"),
                "file_position": int(match.group("pos")),
                "gtid": match.group("gtid"),
            }
            self.log.info("binlog info: %r", self.binlog_info)
        return match

    def _process_output_line_lsn_info(self, line):
        match = self.lsn_re.search(line)
        if match:
            self.lsn_info = {
                "start": int(match.group(1)),
                "end": int(match.group(2)),
            }
            self.log.info("Transaction log lsn info: %r", self.lsn_info)
        return match

    def _update_progress(self, *, last_file_name=None, last_file_size=None):
        estimated_total_bytes = self.data_directory_filtered_size
        estimated_progress = 0
        if estimated_total_bytes > 0:
            estimated_progress = min(self.processed_original_bytes / estimated_total_bytes * 100, 100)

        self.log.info("Processed %s bytes of estimated %s total bytes, progress at %.2f%%",
                      self.processed_original_bytes, estimated_total_bytes, estimated_progress)
        self.stats.gauge_float("myhoard.basebackup.estimated_progress", estimated_progress)

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
            self.stats.unexpected_exception(ex=ex, where="BasebackupOutputReaderThread")
            self.exception = ex
