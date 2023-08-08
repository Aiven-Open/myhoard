# Copyright (c) 2023 Aiven, Helsinki, Finland. https://aiven.io/
from contextlib import contextmanager
from pathlib import Path
from test import random_basic_string
from typing import Iterator

import subprocess


@contextmanager
def mount_tmpfs(path: Path, *, megabytes: int) -> Iterator[Path]:
    """Mount a tmpfs filesystem at the given path and unmount it when done.

    Args:
        path: The path to mount the tmpfs filesystem at (will create a subdirectory there).
        megabytes: The size of the tmpfs filesystem in megabytes.

    Yields:
        The path the tmpfs filesystem was mounted at.
    """
    sub_dir = path / random_basic_string(20, prefix="small_disk_")
    try:
        sub_dir.mkdir(parents=True, exist_ok=True)
        subprocess.check_call(["sudo", "mount", "-t", "tmpfs", "-o", f"size={megabytes}m", "tmpfs", str(sub_dir)])

        yield sub_dir
    finally:
        subprocess.check_call(["sudo", "umount", str(sub_dir)])
