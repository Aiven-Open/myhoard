# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
"""The myhoard daemon with restore download workers that never exit by themselves, for tests that run the daemon as a
subprocess: `python -m test.daemon_with_hung_download_workers --config <file>`."""
from . import hung_download_worker
from myhoard import restore_coordinator
from myhoard.myhoard import main
from unittest.mock import patch

import sys

# Short, so that a test does not wait the real grace on every stop of a restore
DOWNLOAD_WORKER_STOP_GRACE = 0.5

# The download workers are spawned and import this module again, they must not start a daemon of their own
if __name__ == "__main__":
    # patch.object refuses a name its target does not have
    with (
        patch.object(restore_coordinator, "download_binlog", hung_download_worker),
        patch.object(restore_coordinator.RestoreCoordinator, "DOWNLOAD_WORKER_STOP_GRACE", DOWNLOAD_WORKER_STOP_GRACE),
    ):
        sys.exit(main())
