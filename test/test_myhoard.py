# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import contextlib
import json
import os
import signal
import subprocess
import sys

import pytest
import requests

from . import get_random_port, wait_for_port, while_asserts

pytestmark = [pytest.mark.unittest, pytest.mark.all]
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def test_sample_config_keys_match_fixture_config_keys(myhoard_config):
    def validate_recursive(actual_object, expected_object, root=""):
        for key, value in actual_object.items():
            assert key in expected_object, f"Unexpected key {key!r} found under {root!r}"
            if isinstance(value, dict):
                validate_recursive(value, expected_object[key], f"{root}.{key}".lstrip("."))
        for key, value in expected_object.items():
            assert key in actual_object, f"Expected key {key!r} under {root!r} not found"

    with open(os.path.join(ROOT_DIR, "myhoard.json")) as f:
        actual_config = json.load(f)

    validate_recursive(actual_config, myhoard_config)


def test_basic_daemon_execution(myhoard_config):
    config_name = os.path.join(myhoard_config["state_directory"], "myhoard.json")
    with open(config_name, "w") as f:
        json.dump(myhoard_config, f)

    python3 = sys.executable or os.environ.get("PYTHON", "python3")
    cmd = [python3, "-c", "from myhoard.myhoard import main; main()", "--config", config_name, "--log-level", "DEBUG"]
    print("Running command", cmd)
    proc = subprocess.Popen(cmd, env={"PYTHONPATH": ROOT_DIR})
    try:
        http_address = myhoard_config["http_address"]
        http_port = myhoard_config["http_port"]
        wait_for_port(http_port, hostname=http_address, wait_time=5)

        def backups_not_none():
            response = requests.get(f"http://{http_address}:{http_port}/backup", timeout=1)
            assert response.status_code == 200
            assert response.json()["backups"] is not None

        while_asserts(backups_not_none)

        # Update config and see the new config gets applied
        new_http_port = get_random_port(start=3000, end=30000)
        assert new_http_port != http_port
        myhoard_config["http_port"] = new_http_port
        with open(config_name, "w") as f:
            json.dump(myhoard_config, f)

        os.kill(proc.pid, signal.SIGHUP)
        wait_for_port(new_http_port, hostname=http_address, wait_time=2)
        response = requests.get(f"http://{http_address}:{new_http_port}/backup", timeout=1)
        response.raise_for_status()
        assert response.json()["backups"] is not None

        os.kill(proc.pid, signal.SIGINT)
        proc.communicate(input=None, timeout=2)
        assert proc.returncode == 0
    finally:
        with contextlib.suppress(Exception):
            os.kill(proc.pid, signal.SIGKILL)
