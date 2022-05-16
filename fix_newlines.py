#!/usr/bin/env python3
from pathlib import Path
from typing import Collection
import argparse
import io
import subprocess
import sys


def main(exclude_patterns: Collection[str]) -> int:
    has_fixed_a_file = False
    for filename in map(Path, subprocess.check_output(["git", "ls-files"]).decode().splitlines()):
        should_skip_file = False
        for exclude_pattern in exclude_patterns:
            if filename.match(exclude_pattern):
                should_skip_file = True
        if should_skip_file:
            continue
        with filename.open("r+b") as file:
            file.seek(0, io.SEEK_END)
            if file.tell() > 0:
                file.seek(-1, io.SEEK_END)
                if file.read() != b"\n":
                    print(f"Fixed missing newline at end of {filename!s}")
                    file.write(b"\n")
                    has_fixed_a_file = True
    return 1 if has_fixed_a_file else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enforce trailing newlines")
    parser.add_argument("--exclude", dest="exclude", help="file pattern to exclude", action="append")
    args = parser.parse_args()
    sys.exit(main(exclude_patterns=args.exclude))
