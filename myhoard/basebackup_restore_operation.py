# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
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
from contextlib import suppress

from pghoard.common import increase_pipe_capacity, set_stream_nonblocking


class BasebackupRestoreOperation:
    """Restores an existing basebackup. Note that the stream handler callback is
    executed on another thread so that the main thread can manage restore state
    information while the data is being loaded."""

    prepare_completed_re = re.compile(r"Shutdown completed; log sequence number (\d+)$")

    def __init__(
        self,
        *,
        encryption_algorithm,
        encryption_key,
        mysql_config_file_name,
        mysql_data_directory,
        stats,
        stream_handler,
        temp_dir,
    ):
        self.current_file = None
        self.data_directory_size_end = None
        self.data_directory_size_start = None
        self.encryption_algorithm = encryption_algorithm
        self.encryption_key = encryption_key
        self.log = logging.getLogger(self.__class__.__name__)
        self.mysql_config_file_name = mysql_config_file_name
        self.mysql_data_directory = mysql_data_directory
        self.number_of_files = 0
        self.prepared_lsn = None
        self.proc = None
        self.stats = stats
        self.stream_handler = stream_handler
        self.temp_dir = None
        self.temp_dir_base = temp_dir

    def restore_backup(self):
        if os.path.exists(self.mysql_data_directory):
            raise ValueError(f"MySQL data directory {self.mysql_data_directory!r} already exists")

        # Write encryption key to file to avoid having it on command line. NamedTemporaryFile has mode 0600
        with tempfile.NamedTemporaryFile() as encryption_key_file:
            encryption_key_file.write(base64.b64encode(self.encryption_key))
            encryption_key_file.flush()

            cpu_count = multiprocessing.cpu_count()

            self.temp_dir = tempfile.mkdtemp(dir=self.temp_dir_base)

            try:
                # yapf: disable
                command_line = [
                    "xbstream",
                    # TODO: Check if it made sense to restore directly to MySQL data directory so that
                    # we could skip the move phase. It's not clear if move does anything worthwhile
                    # except skips a few extra files, which could instead be deleted explicitly
                    "--directory", self.temp_dir,
                    "--extract",
                    "--decompress",
                    "--decompress-threads", str(max(cpu_count - 1, 1)),
                    "--decrypt", self.encryption_algorithm,
                    "--encrypt-key-file", encryption_key_file.name,
                    "--encrypt-threads", str(max(cpu_count - 1, 1)),
                    "--parallel", str(max(cpu_count - 1, 1)),
                    "--verbose",
                ]

                with self.stats.timing_manager("myhoard.basebackup_restore.xbstream_extract"):
                    with subprocess.Popen(command_line,
                                          bufsize=0,
                                          stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE) as xbstream:
                        self.proc = xbstream
                        self._process_xbstream_input_output()

                self.data_directory_size_start = self._get_directory_size(self.temp_dir)

                # TODO: Get some execution time numbers with non-trivial data sets for --prepare
                # and --move-back commands and add progress monitoring if necessary (and feasible)
                command_line = [
                    "xtrabackup",
                    # defaults file must be given with --defaults-file=foo syntax, space here does not work
                    f"--defaults-file={self.mysql_config_file_name}",
                    "--no-version-check",
                    "--prepare",
                    "--target-dir", self.temp_dir,
                ]
                with self.stats.timing_manager("myhoard.basebackup_restore.xtrabackup_prepare"):
                    with subprocess.Popen(
                        command_line, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    ) as prepare:
                        self.proc = prepare
                        self._process_prepare_input_output()

                # As of Percona XtraBackup 8.0.5 the backup contains binlog.index and one binlog that --move-back
                # tries to restore to appropriate location, presumable with the intent of making MySQL patch gtid_executed
                # table based on what's in the log. We already have logic for patching the table and we don't need
                # this logic from XtraBackup. The logic is also flawed as it tries to restore to location that existed
                # on the source server, which may not be valid for destination server. Just delete the files to disable
                # that restoration logic.
                binlog_index = os.path.join(self.temp_dir, "binlog.index")
                if os.path.exists(binlog_index):
                    with open(binlog_index) as f:
                        binlogs = f.read().split("\n")
                    binlogs = [binlog.rsplit("/", 1)[-1] for binlog in binlogs if binlog.strip()]
                    self.log.info("Deleting redundant binlog index %r and binlogs %r before move", binlog_index, binlogs)
                    os.remove(binlog_index)
                    for binlog_name in binlogs:
                        binlog_name = os.path.join(self.temp_dir, binlog_name)
                        if os.path.exists(binlog_name):
                            os.remove(binlog_name)

                command_line = [
                    "xtrabackup",
                    # defaults file must be given with --defaults-file=foo syntax, space here does not work
                    f"--defaults-file={self.mysql_config_file_name}",
                    "--move-back",
                    "--no-version-check",
                    "--target-dir", self.temp_dir,
                ]
                with self.stats.timing_manager("myhoard.basebackup_restore.xtrabackup_move"):
                    with subprocess.Popen(
                        command_line, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    ) as move_back:
                        self.proc = move_back
                        self._process_move_input_output()
            finally:
                shutil.rmtree(self.temp_dir)

        self.data_directory_size_end = self._get_directory_size(self.mysql_data_directory, cleanup=True)

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
        increase_pipe_capacity(self.proc.stdout, self.proc.stderr)

        self._process_output_loop("xtrabackup-move", None, self._process_move_output_line)

    def _process_prepare_input_output(self):
        increase_pipe_capacity(self.proc.stdout, self.proc.stderr)

        self._process_output_loop("xtrabackup-prepare", None, self._process_prepare_output_line)

        if self.prepared_lsn is None:
            raise Exception("Could not read prepared lsn from xtrabackup output")

    def _process_xbstream_input_output(self):
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

    def _process_move_output_line(self, line, _stream_name):
        line = self.decode_output_line(line)
        if not line:
            return

        self.log.info("xtrabackup-move: %r", line)

    def _process_prepare_output_line(self, line, _stream_name):
        line = self.decode_output_line(line)
        if not line:
            return

        if not self._process_prepare_completed_line(line):
            self.log.info("xtrabackup-prepare: %r", line)

    def _process_prepare_completed_line(self, line):
        match = self.prepare_completed_re.search(line)
        if match:
            self.prepared_lsn = int(match.group(1))
            self.log.info("Restored backup prepared, lsn %s", self.prepared_lsn)
        return match

    def _process_xbstream_output_line(self, line, _stream_name):
        line = self.decode_output_line(line)
        if not line:
            return

        if not self._process_xbstream_output_line_new_file(line):
            self.log.info("xbstream: %r", line)

    def _process_xbstream_output_line_new_file(self, line):
        if not line.endswith(".qp.xbcrypt"):
            return False

        self.current_file = line
        self.number_of_files += 1
        self.log.info("Started processing file %r", self.current_file)
        return True


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
