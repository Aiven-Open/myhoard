# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import contextlib
import logging
import os
import shutil
import signal
import subprocess
import sys
from typing import Optional

import pytest

from myhoard.util import (atomic_create_file, change_master_to, mysql_cursor, wait_for_port)
from myhoard.web_server import WebServer

from . import (
    MySQLConfig, build_controller, build_statsd_client, generate_rsa_key_pair, get_mysql_config_options, get_random_port,
    random_basic_string
)

pytest_plugins = "aiohttp.pytest_plugin"

# Force logging to be configured verbose so we can debug tests easily.
_log_level_str = os.getenv("MYHOARD_TEST_LOG_LEVEL", "WARNING")
_test_log_level = logging._nameToLevel[_log_level_str]  # pylint: disable=protected-access
_test_mysqld_log_level = int(os.getenv("MYHOARD_TEST_MYSQLD_LOG_LEVEL", "0"))

root = logging.getLogger()
root.setLevel(_test_log_level)

handler = logging.StreamHandler(sys.stderr)
handler.setLevel(_test_log_level)
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(pathname)s:%(lineno)d:%(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)


@pytest.fixture(scope="session", name="session_tmpdir")
def fixture_session_tmpdir(tmpdir_factory):
    """Create a temporary directory object that's usable in the session scope.  The returned value is a
    function which creates a new temporary directory which will be automatically cleaned up upon exit."""
    tmpdir_obj = tmpdir_factory.mktemp("myhoard.session.tmpdr.")

    def subdir():
        return tmpdir_obj.mkdtemp(rootdir=tmpdir_obj)

    try:
        yield subdir
    finally:
        with contextlib.suppress(Exception):
            tmpdir_obj.remove(rec=1)


@pytest.fixture(scope="function", name="mysql_master")
def fixture_mysql_master(session_tmpdir):
    with mysql_setup_teardown(session_tmpdir, name="master", server_id=1) as config:
        yield config


@pytest.fixture(scope="function", name="mysql_standby1")
def fixture_mysql_standby1(session_tmpdir, mysql_master):
    with mysql_setup_teardown(session_tmpdir, master=mysql_master, name="standby1", server_id=2) as config:
        yield config


@pytest.fixture(scope="function", name="mysql_standby2")
def fixture_mysql_standby2(session_tmpdir, mysql_master):
    with mysql_setup_teardown(session_tmpdir, master=mysql_master, name="standby2", server_id=3) as config:
        yield config


@pytest.fixture(scope="function", name="mysql_empty")
def fixture_mysql_empty(session_tmpdir):
    with mysql_setup_teardown(session_tmpdir, name="empty", server_id=4, empty=True) as config:
        yield config


@contextlib.contextmanager
def mysql_setup_teardown(session_tmpdir, *, empty=False, master: Optional[MySQLConfig] = None, name, server_id):
    config = mysql_initialize_and_start(session_tmpdir, empty=empty, master=master, name=name, server_id=server_id)
    try:
        yield config
    finally:
        if config.proc:
            os.kill(config.proc.pid, signal.SIGKILL)
            config.proc.wait(timeout=10.0)


def mysql_initialize_and_start(
    session_tmpdir, *, empty=False, master: Optional[MySQLConfig] = None, name, server_id
) -> MySQLConfig:
    mysql_basedir = os.environ.get("MYHOARD_MYSQL_BASEDIR")
    if mysql_basedir is None and os.path.exists("/opt/mysql"):
        mysql_basedir = "/opt/mysql"

    mysqld_bin = "/usr/sbin/mysqld"
    if not os.path.exists(mysqld_bin):
        mysqld_bin = "/usr/bin/mysqld"

    test_base_dir = os.path.abspath(os.path.join(session_tmpdir().strpath, name))
    config_path = os.path.join(test_base_dir, "etc")
    config_options = get_mysql_config_options(
        config_path=config_path, name=name, server_id=server_id, test_base_dir=test_base_dir
    )

    config = f"""
[mysqld]
binlog-transaction-dependency-tracking=WRITESET
binlog-format=ROW
datadir={config_options.datadir}
enforce-gtid-consistency=ON
gtid-mode=ON
log-bin={config_options.binlog_file_prefix}
log-bin-index={config_options.binlog_index_file}
log_error_verbosity = {_test_mysqld_log_level}
mysqlx=OFF
pid-file={config_options.pid_file}
port={config_options.port}
read-only={config_options.read_only}
relay-log={config_options.relay_log_file_prefix}
relay-log-index={config_options.relay_log_index_file}
server-id={server_id}
skip-name-resolve=ON
skip-slave-start=ON
slave-parallel-type=LOGICAL_CLOCK
slave-parallel-workers={config_options.parallel_workers}
slave-preserve-commit-order=ON
socket={config_options.datadir}/mysql.sock
sql-mode=ANSI,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
transaction-write-set-extraction=XXHASH64

[validate_password]
policy=LOW
"""

    config_file = os.path.join(config_path, "my.cnf")
    with atomic_create_file(config_file, perm=0o644) as f:
        f.write(config)

    password = random_basic_string()
    init_file = os.path.join(config_path, "init_file.sql")

    init_config = f"""
DROP USER IF EXISTS 'root'@'localhost';
CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY '{password}';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION;
FLUSH PRIVILEGES;
"""

    with atomic_create_file(init_file, perm=0o644) as f:
        f.write(init_config)

    if not empty:
        # Since data directory is empty to begin with we first need to start mysqld with --initialize switch,
        # which creates necessary files and exists once done. We don't want any binlog or GTIDs to be generated
        # for these operations so run without binlog and GTID
        cmd = [
            mysqld_bin,
            f"--defaults-file={config_file}",
        ]
        if mysql_basedir:
            cmd.append(f"--basedir={mysql_basedir}")
        cmd.extend([
            "--initialize",
            "--disable-log-bin",
            "--gtid-mode=OFF",
            "--skip-slave-preserve-commit-order",
            "--init-file",
            init_file,
        ])
        proc = subprocess.Popen(cmd)
        proc.wait(timeout=30)

    connect_options = {
        "host": "127.0.0.1",
        "password": password,
        "port": config_options.port,
        "timeout": 10,
        "user": "root",
    }

    cmd = [mysqld_bin, f"--defaults-file={config_file}"]
    if mysql_basedir:
        cmd.append(f"--basedir={mysql_basedir}")
    if empty:
        # Empty server is used for restoring data. Wipe data directory and don't start the server
        shutil.rmtree(config_options.datadir)
        proc = None
    else:
        proc = subprocess.Popen(cmd)
        wait_for_port(host="127.0.0.1", port=config_options.port, timeout=30.0)
        # Ensure connecting to the newly started server works and if this is standby also start replication
        with mysql_cursor(**connect_options) as cursor:
            if master:
                change_master_to(
                    cursor=cursor,
                    options={
                        "MASTER_AUTO_POSITION": 1,
                        "MASTER_CONNECT_RETRY": 0.1,
                        "MASTER_HOST": "127.0.0.1",
                        "MASTER_PORT": master.port,
                        "MASTER_PASSWORD": master.password,
                        "MASTER_SSL": 0,
                        "MASTER_USER": master.user,
                    }
                )
                cursor.execute("START SLAVE IO_THREAD, SQL_THREAD")
            else:
                cursor.execute("SELECT 1")

    return MySQLConfig(
        base_dir=test_base_dir,
        config=config,
        config_name=config_file,
        config_options=config_options,
        connect_options=connect_options,
        password=password,
        port=config_options.port,
        proc=proc,
        server_id=server_id,
        startup_command=cmd,
        user="root",
    )


@pytest.fixture(scope="function", name="encryption_keys")
def fixture_encryption_keys():
    private_key, public_key = generate_rsa_key_pair()
    yield {
        "private": private_key.decode("ascii"),
        "public": public_key.decode("ascii"),
    }


@pytest.fixture(scope="function", name="default_backup_site")
def fixture_default_backup_site(session_tmpdir, encryption_keys):
    backup_dir = os.path.abspath(os.path.join(session_tmpdir().strpath, "backups"))
    os.makedirs(backup_dir)
    backup_site = {
        "compression": {
            "algorithm": "snappy",
        },
        "encryption_keys": encryption_keys,
        "object_storage": {
            "directory": backup_dir,
            "storage_type": "local",
        },
        "recovery_only": False,
    }
    yield backup_site


@pytest.fixture(scope="function", name="master_controller")
def fixture_master_controller(session_tmpdir, mysql_master, default_backup_site):
    controller = build_controller(
        default_backup_site=default_backup_site,
        mysql_config=mysql_master,
        session_tmpdir=session_tmpdir,
    )
    try:
        yield controller, mysql_master
    finally:
        controller.stop()


@pytest.fixture(scope="function", name="standby1_controller")
def fixture_standby1_controller(session_tmpdir, mysql_standby1, default_backup_site):
    controller = build_controller(
        default_backup_site=default_backup_site,
        mysql_config=mysql_standby1,
        session_tmpdir=session_tmpdir,
    )
    try:
        yield controller, mysql_standby1
    finally:
        controller.stop()


@pytest.fixture(scope="function", name="standby2_controller")
def fixture_standby2_controller(session_tmpdir, mysql_standby2, default_backup_site):
    controller = build_controller(
        default_backup_site=default_backup_site,
        mysql_config=mysql_standby2,
        session_tmpdir=session_tmpdir,
    )
    try:
        yield controller, mysql_standby2
    finally:
        controller.stop()


@pytest.fixture(scope="function", name="myhoard_config")
def fixture_myhoard_config(default_backup_site, mysql_master, session_tmpdir):
    state_dir = os.path.abspath(os.path.join(session_tmpdir().strpath, "myhoard_state"))
    os.makedirs(state_dir)
    temp_dir = os.path.abspath(os.path.join(session_tmpdir().strpath, "temp"))
    os.makedirs(temp_dir)
    return {
        "backup_settings": {
            "backup_age_days_max": 14,
            "backup_count_max": 100,
            "backup_count_min": 14,
            "backup_hour": 3,
            "backup_interval_minutes": 1440,
            "backup_minute": 0,
            "forced_binlog_rotation_interval": 300,
            "upload_site": "default",
        },
        "backup_sites": {
            "default": default_backup_site,
        },
        "binlog_purge_settings": {
            "enabled": True,
            "min_binlog_age_before_purge": 600,
            "purge_interval": 60,
            "purge_when_observe_no_streams": True
        },
        "http_address": "127.0.0.1",
        "http_port": get_random_port(start=3000, end=30000),
        "mysql": {
            "binlog_prefix": mysql_master.config_options.binlog_file_prefix,
            "client_params": {
                "host": "127.0.0.1",
                "password": "NgLqvU8gbWCtfJWJPy",
                "port": 3306,
                "user": "root"
            },
            "config_file_name": mysql_master.config_name,
            "data_directory": mysql_master.config_options.datadir,
            "relay_log_index_file": mysql_master.config_options.relay_log_index_file,
            "relay_log_prefix": mysql_master.config_options.relay_log_file_prefix,
        },
        "restore_max_binlog_bytes": 4294967296,
        "sentry_dsn": None,
        "server_id": mysql_master.server_id,
        "start_command": mysql_master.startup_command,
        "state_directory": state_dir,
        "statsd": {
            "host": None,
            "port": None,
            "tags": {
                "app": "myhoard",
            },
        },
        "systemctl_command": ["sudo", "/usr/bin/systemctl"],
        "systemd_env_update_command": [
            "sudo", "/usr/bin/myhoard_mysql_env_update", "--", "/etc/systemd/system/mysqld.environment", "MYSQLD_OPTS"
        ],
        "systemd_service": None,
        "temporary_directory": temp_dir
    }


@pytest.fixture(scope="function", name="web_client")
async def fixture_web_client(master_controller, aiohttp_client):
    server = WebServer(
        controller=master_controller[0],
        http_address="::1",
        http_port=-1,
        stats=build_statsd_client(),
    )
    client = await aiohttp_client(server.app)
    yield client
