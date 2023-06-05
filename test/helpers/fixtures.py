# Copyright (c) 2023 Aiven, Helsinki, Finland. https://aiven.io/
from contextlib import contextmanager
from myhoard.controller import BackupSiteInfo, Controller
from pathlib import Path
from py.path import local as LocalPath
from test import build_controller, MySQLConfig
from test.helpers.filesystem import mount_tmpfs
from typing import Callable, Iterator


@contextmanager
def create_controller_in_small_disk(
    *, session_tmpdir: Callable[[], LocalPath], mysql_config: MySQLConfig, default_backup_site: BackupSiteInfo
) -> Iterator[tuple[Controller, MySQLConfig]]:
    sub_dir = Path(session_tmpdir().strpath) / "small_disk"
    with mount_tmpfs(path=sub_dir, megabytes=8) as tmpfs_dir:
        controller = build_controller(
            Controller,
            default_backup_site=default_backup_site,
            mysql_config=mysql_config,
            session_tmpdir=lambda: LocalPath(tmpfs_dir),
        )

        try:
            yield controller, mysql_config

        finally:
            controller.stop()
