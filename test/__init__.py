# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import asyncio
import contextlib
import multiprocessing
import os
import random
import signal
import socket
import string
import subprocess
import threading
import time

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import myhoard.util as myhoard_util
from myhoard.backup_stream import BackupStream
from myhoard.controller import Controller
from myhoard.statsd import StatsClient


class MySQLConfig:
    def __init__(
        self,
        *,
        base_dir=None,
        config=None,
        config_name=None,
        config_options=None,
        connect_options=None,
        password=None,
        port=None,
        proc=None,
        server_id=None,
        startup_command=None,
        user=None
    ):
        self.base_dir = base_dir
        self.config = config
        self.config_name = config_name
        self.config_options = config_options
        self.connect_options = connect_options
        self.password = password
        self.port = port
        self.proc = proc
        self.server_id = server_id
        self.startup_command = startup_command
        self.user = user


def build_controller(
    *, cls=None, default_backup_site, mysql_config: MySQLConfig, session_tmpdir, state_dir=None, temp_dir=None
):
    Controller.ITERATION_SLEEP = 0.1
    Controller.BACKUP_REFRESH_INTERVAL_BASE = 0.1
    Controller.BACKUP_REFRESH_ACTIVE_MULTIPLIER = 1
    BackupStream.ITERATION_SLEEP = 0.1
    BackupStream.REMOTE_POLL_INTERVAL = 0.1

    state_dir = state_dir or os.path.abspath(os.path.join(session_tmpdir().strpath, "myhoard_state"))
    os.makedirs(state_dir, exist_ok=True)
    temp_dir = temp_dir or os.path.abspath(os.path.join(session_tmpdir().strpath, "temp"))
    os.makedirs(temp_dir, exist_ok=True)

    cls = cls or Controller
    controller = cls(
        backup_settings={
            "backup_age_days_max": 14,
            "backup_count_max": 100,
            "backup_count_min": 14,
            "backup_hour": 3,
            "backup_interval_minutes": 1440,
            "backup_minute": 0,
            "forced_binlog_rotation_interval": 300,
        },
        backup_sites={"default": default_backup_site},
        binlog_purge_settings={
            "enabled": True,
            "min_binlog_age_before_purge": 30,
            "purge_interval": 1,
            "purge_when_observe_no_streams": True,
        },
        mysql_binlog_prefix=mysql_config.config_options.binlog_file_prefix,
        mysql_client_params=mysql_config.connect_options,
        mysql_config_file_name=mysql_config.config_name,
        mysql_data_directory=mysql_config.config_options.datadir,
        mysql_relay_log_index_file=mysql_config.config_options.relay_log_index_file,
        mysql_relay_log_prefix=mysql_config.config_options.relay_log_file_prefix,
        restart_mysqld_callback=lambda **kwargs: restart_mysql(mysql_config, **kwargs),
        restore_max_binlog_bytes=2 * 1024 * 1024,
        server_id=mysql_config.server_id,
        state_dir=state_dir,
        stats=build_statsd_client(),
        temp_dir=temp_dir,
    )
    return controller


def build_statsd_client():
    return StatsClient(host=None, port=None, tags=None)


class MySQLConfigOptions:
    def __init__(
        self,
        binlog_file_prefix,
        binlog_index_file,
        datadir,
        parallel_workers,
        pid_file,
        port,
        read_only,
        relay_log_file_prefix,
        relay_log_index_file,
        server_id,
    ):
        self.binlog_file_prefix = binlog_file_prefix
        self.binlog_index_file = binlog_index_file
        self.datadir = datadir
        self.parallel_workers = parallel_workers
        self.pid_file = pid_file
        self.port = port
        self.read_only = read_only
        self.relay_log_file_prefix = relay_log_file_prefix
        self.relay_log_index_file = relay_log_index_file
        self.server_id = server_id


def get_mysql_config_options(*, config_path, name, server_id, test_base_dir) -> MySQLConfigOptions:
    os.makedirs(config_path)
    data_dir = os.path.join(test_base_dir, "data")
    os.makedirs(data_dir)
    binlog_dir = os.path.join(test_base_dir, "binlogs")
    os.makedirs(binlog_dir)
    relay_log_dir = os.path.join(test_base_dir, "relay_logs")
    os.makedirs(relay_log_dir)

    port = get_random_port()
    return MySQLConfigOptions(
        binlog_file_prefix=os.path.join(binlog_dir, "bin"),
        binlog_index_file=os.path.join(test_base_dir, "binlog.index"),
        datadir=data_dir,
        parallel_workers=multiprocessing.cpu_count(),
        pid_file=os.path.join(config_path, "mysql.pid"),
        port=port,
        read_only=name != "master",
        relay_log_file_prefix=os.path.join(relay_log_dir, "relay"),
        relay_log_index_file=os.path.join(test_base_dir, "relay_log.index"),
        server_id=server_id,
    )


def restart_mysql(mysql_config, *, with_binlog=True, with_gtids=True):
    if mysql_config.proc:
        proc = mysql_config.proc
        mysql_config.proc = None
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=20.0)
        print("Stopped mysqld with pid", proc.pid)
    command = mysql_config.startup_command
    if not with_binlog:
        command = command + ["--disable-log-bin", "--skip-slave-preserve-commit-order"]
    if not with_gtids:
        command = command + ["--gtid-mode=OFF"]
    mysql_config.proc = subprocess.Popen(command)
    print("Started mysqld with pid", mysql_config.proc.pid)
    wait_for_port(mysql_config.port, wait_time=10)


def port_is_listening(hostname, port, ipv6):
    if ipv6:
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 0)
    else:
        s = socket.socket()
    s.settimeout(0.5)
    try:
        s.connect((hostname, port))
        return True
    except socket.error:
        return False


def wait_for_port(port, *, hostname="127.0.0.1", wait_time=20.0, ipv6=False):
    start_time = time.monotonic()
    while True:
        if port_is_listening(hostname, port, ipv6):
            break
        elapsed = time.monotonic() - start_time
        if elapsed >= wait_time:
            raise Exception(f"Port {port} not listening after {wait_time} seconds")
        time.sleep(0.1)


def get_random_port(*, start=3000, end=30000):
    while True:
        port = random.randint(start, end)
        if not port_is_listening("127.0.0.1", port, True) and not port_is_listening("127.0.0.1", port, False):
            return port


def random_basic_string(length=16, *, prefix=None, digit_spacing=None):
    if prefix is None:
        prefix = random.choice(string.ascii_lowercase)
    random_length = length - len(prefix)
    if digit_spacing is None:
        chars = [random.choice(string.ascii_lowercase + string.digits) for _ in range(random_length)]
    else:
        chars = [
            random.choice(string.ascii_lowercase if (n % (digit_spacing + 1)) > 0 else string.digits)
            for n in range(random_length)
        ]
    return "{}{}".format(prefix, "".join(chars))


def generate_rsa_key_pair(*, bits=3072, public_exponent=65537):
    private = rsa.generate_private_key(public_exponent=public_exponent, key_size=bits, backend=default_backend())
    public = private.public_key()

    private_pem = private.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    public_pem = public.public_bytes(
        encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    return private_pem, public_pem


def wait_for_condition(condition, *, timeout=5.0, interval=0.1):
    start_time = time.monotonic()
    while True:
        if time.monotonic() - start_time >= timeout:
            raise Exception(f"Timeout of {timeout}s exceeded before condition was met")
        if condition():
            break
        time.sleep(interval)


def while_asserts(condition, *, timeout=5.0, interval=0.1):
    last_exception = AssertionError("for static checker")
    start_time = time.monotonic()
    while True:
        if time.monotonic() - start_time >= timeout:
            raise last_exception
        try:
            condition()
            break
        except AssertionError as ex:
            last_exception = ex
            time.sleep(interval)


async def awhile_asserts(condition, *, timeout=5.0, interval=0.1):
    last_exception = AssertionError("for static checker")
    start_time = time.monotonic()
    while True:
        if time.monotonic() - start_time >= timeout:
            raise last_exception
        try:
            await condition()
            break
        except AssertionError as ex:
            last_exception = ex
            await asyncio.sleep(interval)


class DataGenerator(threading.Thread):
    """Generates data into MySQL in busy loop. Used to validate that all data is correctly backed up"""

    def __init__(self, *, connect_info, index_offset=0, make_temp_tables=True):
        super().__init__()
        self.basic_wait = 0.1
        self.committed_row_count = 0
        self.connect_info = connect_info
        self.estimated_bytes = 0
        self.generate_data_event = threading.Event()
        self.generate_data_event.set()
        self.index_offset = index_offset
        self.is_running = True
        self.make_temp_tables = make_temp_tables
        self.paused = False
        self.pending_row_count = 0
        self.row_count = 0
        self.row_infos = []
        self.temp_table_index = 0
        self.temp_tables = []

    def run(self):
        with myhoard_util.mysql_cursor(**self.connect_info) as cursor1:
            with myhoard_util.mysql_cursor(**self.connect_info) as cursor2:
                cursor1.execute("CREATE DATABASE IF NOT EXISTS db1")
                cursor1.execute("CREATE TABLE IF NOT EXISTS db1.t1 (id INTEGER PRIMARY KEY, data TEXT)")
                while self.is_running:
                    if not self.generate_data_event.wait(timeout=0.1):
                        self.commit_pending(cursor1)
                        self.paused = True
                        continue

                    self.paused = False
                    self.direct_data_generate(cursor1)
                    if self.make_temp_tables:
                        self.indirect_data_generate(cursor2)
                    time.sleep(self.basic_wait)

                self.commit_pending(cursor1)

                for table_name in self.temp_tables:
                    print("Inserting rows from temp table", table_name)
                    cursor2.execute(f"INSERT INTO db1.t1 (id, data) SELECT id, data FROM {table_name}")
                    cursor2.execute(f"DROP TEMPORARY TABLE {table_name}")
                    cursor2.execute("COMMIT")
                cursor1.execute("FLUSH BINARY LOGS")

    def stop(self):
        self.is_running = False
        with contextlib.suppress(Exception):
            self.join()

    def commit_pending(self, cursor):
        if not self.pending_row_count:
            return

        self.committed_row_count += self.pending_row_count
        self.pending_row_count = 0
        cursor.execute("COMMIT")

    def direct_data_generate(self, cursor):
        do_commit = random.random() < self.basic_wait * 3
        do_flush = random.random() < self.basic_wait * 2
        self.pending_row_count += self.generate_rows(cursor, "db1.t1")
        if do_commit:
            self.commit_pending(cursor=cursor)
        if do_flush:
            self.committed_row_count += self.pending_row_count
            self.pending_row_count = 0
            cursor.execute("FLUSH BINARY LOGS")

    def indirect_data_generate(self, cursor):
        table_name = f"db1.temp_t{self.temp_table_index}"
        print("Creating temp table", table_name, "start identifier", self.row_count + self.index_offset + 1)
        self.temp_table_index += 1
        cursor.execute(f"CREATE TEMPORARY TABLE {table_name} (id INTEGER, data TEXT)")
        self.temp_tables.append(table_name)
        self.generate_rows(cursor, table_name)

        drop_table = random.random() < 0.9
        if drop_table and len(self.temp_tables) > 20:
            index = random.randrange(0, len(self.temp_tables))
            table_name = self.temp_tables[index]
            self.temp_tables.pop(index)
            print("Inserting rows from temp table", table_name)
            cursor.execute(f"INSERT INTO db1.t1 (id, data) SELECT id, data FROM {table_name}")
            cursor.execute(f"DROP TEMPORARY TABLE {table_name}")
            cursor.execute("COMMIT")
            cursor.execute("SELECT @@GLOBAL.gtid_executed AS gtid_executed")
            gtid_executed = cursor.fetchone()["gtid_executed"]
            print("GTID executed after", table_name, "insert:", gtid_executed)

    def generate_rows(self, cursor, table):
        row_count = random.randrange(50, 200)
        for _ in range(row_count):
            character = random.choice("abcdefghijklmnopqrstuvwxyz")
            character_count = random.randrange(10, 10000)
            self.row_infos.append((character, character_count))
            self.row_count += 1
            self.estimated_bytes += character_count + 10
            data = character * character_count
            cursor.execute(f"INSERT INTO {table} (id, data) VALUES (%s, %s)", (self.row_count + self.index_offset, data))
        return row_count
