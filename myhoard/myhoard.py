# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from myhoard import version
from myhoard.controller import Controller
from myhoard.statsd import StatsClient
from myhoard.util import (
    DEFAULT_XTRABACKUP_SETTINGS,
    detect_running_process_id,
    find_extra_xtrabackup_executables,
    parse_dow_schedule,
    wait_for_port,
)
from myhoard.web_server import WebServer

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys

try:
    from systemd import daemon  # pylint: disable=no-name-in-module
except ImportError:
    daemon = None


class MyHoard:
    def __init__(self, config_file):
        self.config = {}
        self.config_file = config_file
        self.config_reload_pending = True
        self.controller = None
        self.is_running = True
        self.log = logging.getLogger(self.__class__.__name__)
        self.loop = asyncio.get_event_loop()
        self.mysqld_pid = None
        self.reload_retry_interval = 10
        self.reloading = False
        self.systemd_notified = False
        self.web_server = None

    def request_reload(self, _signal=None, _frame=None):
        self.log.info("Got SIGHUP signal, marking config reload pending")
        asyncio.ensure_future(self._reload_and_initialize_if_possible())

    def request_shutdown(self, is_signal=True, _frame=None):
        if is_signal:
            self.log.info("Got SIGINT or SIGTERM signal, shutting down")
        self.loop.stop()

    async def _reload_and_initialize_if_possible(self):
        if self.controller and not self.controller.is_safe_to_reload():
            self.log.info("Reload requested but controller state does not allow safe reload, postponing")
            await asyncio.sleep(self.reload_retry_interval)
            asyncio.ensure_future(self._reload_and_initialize_if_possible())
            return

        await self._reload_and_initialize()

    async def _reload_and_initialize(self):
        if self.reloading:
            self.log.info("Reload called while already reloading configuration")
            await asyncio.sleep(0.1)
            asyncio.ensure_future(self._reload_and_initialize_if_possible())
            return

        self.reloading = True
        try:
            await self._stop()
            self._load_configuration()
            await self._start()
        finally:
            self.reloading = False

    def run(self):
        self.loop.add_signal_handler(signal.SIGHUP, self.request_reload)
        self.loop.add_signal_handler(signal.SIGINT, self.request_shutdown)
        self.loop.add_signal_handler(signal.SIGTERM, self.request_shutdown)
        self.loop.run_until_complete(self._reload_and_initialize())

        self.loop.run_forever()
        self.loop.run_until_complete(self._stop())
        self.log.info("Exiting")

        return 0

    def _load_configuration(self):
        with open(self.config_file, "r") as f:
            self.config = json.load(f)

        start_command = self.config.get("start_command")
        systemd_service = self.config.get("systemd_service")
        if start_command and systemd_service:
            raise Exception("Only one of 'start_command' and 'systemd_service' must be specified")
        if not start_command and not systemd_service:
            raise Exception("Either 'start_command' or 'systemd_service' must be specified")
        if start_command and not isinstance(start_command, list):
            raise Exception("'start_command' must be a list")

        backup_settings = self.config["backup_settings"]
        ival = backup_settings["backup_interval_minutes"]
        if (ival > 1440 and ival // 1440 * 1440 != ival) or (ival < 1440 and 1440 // ival * ival != 1440):
            raise Exception("Backup interval must be 1440, multiple of 1440, or integer divisor of 1440")

        incremental = backup_settings.get("incremental", {})
        if incremental and incremental.get("enabled", False):
            dow_schedule = incremental.get("full_backup_week_schedule")
            if not dow_schedule:
                raise ValueError("Incremental backups require `full_backup_week_schedule`")
            parse_dow_schedule(dow_schedule)

        if self.config["http_address"] not in {"127.0.0.1", "::1", "localhost"}:
            self.log.warning("Binding to non-localhost address %r is highly discouraged", self.config["http_address"])

        extra_pxb_bins = find_extra_xtrabackup_executables()
        if extra_pxb_bins:
            self.log.info("Found extra xtrabackup binaries: %r", extra_pxb_bins)

        self.log.info("Configuration loaded")

    def _notify_systemd(self):
        if self.systemd_notified:
            return

        if daemon:
            daemon.notify("READY=1")

        self.systemd_notified = True

    def _restart_mysqld(self, *, with_binlog, with_gtids):
        systemd_service = self.config.get("systemd_service")
        if systemd_service:
            self._restart_systemd(with_binlog=with_binlog, with_gtids=with_gtids, service=systemd_service)
        else:
            mysqld_options = []
            if not with_binlog:
                mysqld_options.append("--disable-log-bin")
                # If config says slave-preserve-commit-order=ON MySQL would refuse to start if binlog is
                # disabled. To prevent that from happening ensure preserve commit order is disabled
                mysqld_options.append("--skip-slave-preserve-commit-order")
            if not with_gtids:
                mysqld_options.append("--gtid-mode=OFF")
            self._restart_process(mysqld_options=mysqld_options)

        # Ensure the server is accepting connections
        params = self.config["mysql"]["client_params"]
        wait_for_port(host=params["host"], port=params["port"], timeout=15)

    def _restart_process(self, *, mysqld_options):
        # When not using systemd and we haven't started mysqld (during current invocation of the daemon)
        # start by determining current pid (if any) of the process so that we can kill it before starting.
        if self.mysqld_pid is None:
            self.mysqld_pid, dummy_output_bytes = detect_running_process_id(" ".join(self.config["start_command"])) or -1
        if self.mysqld_pid and self.mysqld_pid > 0:
            self.log.info("Terminating running mysqld process %s", self.mysqld_pid)
            os.kill(self.mysqld_pid, signal.SIGTERM)
            os.waitpid(self.mysqld_pid, 0)
            self.log.info("Process %s exited", self.mysqld_pid)
            self.mysqld_pid = -1

        full_command = self.config["start_command"] + mysqld_options
        self.log.info("Starting process %r", full_command)
        proc = subprocess.Popen(  # pylint: disable=consider-using-with
            full_command, env={"MYSQLD_OPTS": " ".join(mysqld_options)}
        )
        self.mysqld_pid = proc.pid
        self.log.info("Process %r started, pid %s", full_command, proc.pid)

    def _restart_systemd(self, with_binlog, with_gtids, service):
        self.log.info("Restarting service %r", service)

        command = self.config["systemd_env_update_command"].copy()
        command.extend(["-b", "true"] if with_binlog else ["-b", "false"])
        command.extend(["-g", "true"] if with_gtids else ["-g", "false"])
        try:
            subprocess.run(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE, check=True)
        except subprocess.CalledProcessError as e:
            self.log.error(
                "Failed to update MySQL config, %r exited with code %s. Output: %r / %r",
                command,
                e.returncode,
                e.output,
                e.stderr,
            )
            raise Exception(f"Reconfiguring {service!r} failed. Code {e.returncode}") from e

        systemctl = self.config["systemctl_command"]
        try:
            subprocess.run(systemctl + ["restart", service], stderr=subprocess.PIPE, stdout=subprocess.PIPE, check=True)
        except subprocess.CalledProcessError as e:
            self.log.error(
                "Failed to restart %r, systemctl exited with code %s. Output: %r / %r",
                service,
                e.returncode,
                e.output,
                e.stderr,
            )
            raise Exception(f"Restarting {service!r} failed. Code {e.returncode}") from e
        self.log.info("Restarting %r completed successfully", service)

    async def _start(self):
        statsd_config = self.config["statsd"]
        statsd = StatsClient(
            host=statsd_config["host"],
            port=statsd_config["port"],
            sentry_dsn=self.config["sentry_dsn"],
            tags=statsd_config["tags"],
        )
        mysql = self.config["mysql"]
        self.controller = Controller(
            backup_settings=self.config["backup_settings"],
            backup_sites=self.config["backup_sites"],
            binlog_purge_settings=self.config["binlog_purge_settings"],
            mysql_binlog_prefix=mysql["binlog_prefix"],
            mysql_client_params=mysql["client_params"],
            mysql_config_file_name=mysql["config_file_name"],
            mysql_data_directory=mysql["data_directory"],
            mysql_relay_log_index_file=mysql["relay_log_index_file"],
            mysql_relay_log_prefix=mysql["relay_log_prefix"],
            optimize_tables_before_backup=self.config.get("optimize_tables_before_backup", False),
            restart_mysqld_callback=self._restart_mysqld,
            restore_max_binlog_bytes=self.config["restore_max_binlog_bytes"],
            restore_free_memory_percentage=self.config.get("restore_free_memory_percentage"),
            server_id=self.config["server_id"],
            state_dir=self.config["state_directory"],
            stats=statsd,
            temp_dir=self.config["temporary_directory"],
            xtrabackup_settings=self.config.get("xtrabackup", DEFAULT_XTRABACKUP_SETTINGS),
            auto_mark_backups_broken=self.config.get("restore_auto_mark_backups_broken", False),
        )
        self.controller.start()
        self.web_server = WebServer(
            controller=self.controller,
            http_address=self.config["http_address"],
            http_port=self.config["http_port"],
            stats=statsd,
        )
        await self.web_server.start()
        self._notify_systemd()

    async def _stop(self):
        web_server = self.web_server
        self.web_server = None
        if web_server:
            await web_server.stop()
        controller = self.controller
        self.controller = None
        if controller:
            controller.stop()


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(prog="myhoard", description="MySQL backup and restore daemon")
    parser.add_argument("--version", action="version", help="show program version", version=version.__version__)
    parser.add_argument("--log-level", help="Log level", default="INFO", choices=("ERROR", "WARNING", "INFO", "DEBUG"))
    parser.add_argument("--config", help="Configuration file path", default=os.environ.get("MYHOARD_CONFIG"))
    arg = parser.parse_args(args)

    if not arg.config:
        print("config file path must be given with --config or via env MYHOARD_CONFIG", file=sys.stderr)
        return 1

    logging.basicConfig(level=arg.log_level, format="%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s")

    hoard = MyHoard(arg.config)
    return hoard.run()


if __name__ == "__main__":
    sys.exit(main())
