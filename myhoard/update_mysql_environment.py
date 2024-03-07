# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from myhoard.util import atomic_create_file

import argparse
import logging
import sys


class EnvironmentUpdater:
    """Updates given environment variable in given systemd environment variable file.
    Implemented as separate command to allow executing as root via sudo"""

    def __init__(self, args):
        self.args = args
        self.log = logging.getLogger(self.__class__.__name__)

    def update(self):
        # make sure we only update the parameter we need to update i.e., MYSQLD_OPTS
        key = "MYSQLD_OPTS"  # we only update this environment variable
        options = []
        if self.args.with_bin_log != "true":
            options.append("--disable-log-bin")
            # If config says slave-preserve-commit-order=ON MySQL would refuse to start if binlog is
            # disabled. To prevent that from happening ensure preserve commit order is disabled
            options.append("--skip-slave-preserve-commit-order")
        if self.args.gtid_mode != "true":
            options.append("--gtid-mode=OFF")
        try:
            with open(self.args.env_file, "r") as f:
                contents = [line.rstrip("\n") for line in f.readlines() if line.strip() and not line.startswith(key)]
        except FileNotFoundError:
            contents = []
        value = " ".join(options)
        if value:
            contents.append(f"{key}={value}")
        with atomic_create_file(self.args.env_file) as f:
            f.write("\n".join(contents) + "\n")


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Aiven MySQL environment updater")
    parser.add_argument("-f", dest="env_file", metavar="FILE", help="The Environment file to be updated")
    parser.add_argument("-b", dest="with_bin_log", choices=["true", "false"], help="Flag to enable bin log or not")
    parser.add_argument("-g", dest="gtid_mode", choices=["true", "false"], help="Flag to turn GTID mode on or off")
    args = parser.parse_args()
    EnvironmentUpdater(args).update()
    return 0


if __name__ == "__main__":
    sys.exit(main())
