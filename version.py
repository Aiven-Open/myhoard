# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys

PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))


def save_version(new_version: str | None, old_version: str | None, version_file_path: str) -> bool:
    if not new_version:
        return False
    if not old_version or new_version != old_version:
        with open(version_file_path, "w") as fp:
            fp.write(f'__version__ = "{new_version}"\n')
    return True


def get_project_version(version_file_relpath: str) -> str:
    version_file_abspath = os.path.join(PROJECT_DIR, version_file_relpath)
    file_version = None
    try:
        module_spec = importlib.util.spec_from_file_location("verfile", location=version_file_abspath)
        if module_spec:
            module = importlib.util.module_from_spec(module_spec)
            if module_spec.loader:
                module_spec.loader.exec_module(module)
                file_version = module.__version__
        else:
            print(f"Could not load module spec from version file location: {version_file_abspath!r}")
    except IOError:
        print(f"Could not load version module from spec (file location: {version_file_abspath!r})")

    os.chdir(os.path.dirname(__file__) or ".")
    try:
        git_out = subprocess.check_output(["git", "describe", "--always"], stderr=getattr(subprocess, "DEVNULL", None))
    except (OSError, subprocess.CalledProcessError):
        pass
    else:
        git_version = git_out.splitlines()[0].strip().decode("utf-8")
        if "." not in git_version:
            git_version = f"0.0.1-0-unknown-{git_version}"
        if save_version(git_version, file_version, version_file_abspath):
            print(f"Version resolved from git: {git_version}")
            return git_version

    short_version = subprocess.run(["git", "describe", "--abbrev=0"], check=True, text=True, capture_output=True).stderr
    if save_version(short_version, file_version, version_file_abspath):
        print(f"Short version resolved from git abbrev: {short_version}")
        return short_version

    if not file_version:
        raise Exception(f"version not available from git or from file {version_file_abspath!r}")
    else:
        print(f"Version resolved from file: {file_version}")
    return file_version


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 version.py <filename>")
        sys.exit(1)
    get_project_version(sys.argv[1])
