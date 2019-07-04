# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
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
        try:
            with open(self.args.environment_file, "r") as f:
                contents = [
                    line.rstrip("\n") for line in f.readlines() if line.strip() and not line.startswith(self.args.key)
                ]
        except FileNotFoundError:
            contents = []

        if self.args.value:
            contents.append(f"{self.args.key}={self.args.value}")
        with open(self.args.environment_file, "w+") as f:
            f.write("\n".join(contents) + "\n")


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Aiven MySQL environment updater")
    parser.add_argument("environment_file", metavar="FILE", help="Environment file")
    parser.add_argument("key", help="Environment variable name")
    parser.add_argument("value", help="Environment variable value")
    args = parser.parse_args()
    EnvironmentUpdater(args).update()
    return 0


if __name__ == "__main__":
    sys.exit(main())
