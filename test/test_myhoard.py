# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from . import get_random_port, wait_for_port, while_asserts
from unittest.mock import MagicMock, patch

import contextlib
import json
import os
import pytest
import requests
import signal
import subprocess
import sys

pytestmark = [pytest.mark.unittest, pytest.mark.all]
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def mock_stderr_isatty():
    """Returns a context manager to mock sys.stderr.isatty() for testing logging behavior."""

    @contextlib.contextmanager
    def _mock_stderr_isatty(is_tty: bool):
        mock_stderr = MagicMock()
        mock_stderr.isatty.return_value = is_tty
        # Patch sys.stderr in the myhoard.myhoard module namespace
        with patch("myhoard.myhoard.sys.stderr", mock_stderr):
            yield

    return _mock_stderr_isatty


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
    cmd = [
        python3,
        "-c",
        "from myhoard.myhoard import main; main()",
        "--config",
        config_name,
        "--log-level",
        "DEBUG",
    ]
    print("Running command", cmd)
    with subprocess.Popen(cmd, env={"PYTHONPATH": ROOT_DIR}) as proc:
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


def test_main_logging_uses_journal_handler_when_available_and_not_tty(
    tmp_path, mock_stderr_isatty
):  # pylint: disable=redefined-outer-name
    """Verify JournalHandler is used when systemd.journal is available and stderr is not a tty."""
    from myhoard.myhoard import main

    config_name = tmp_path / "myhoard.json"
    minimal_config = {"backup_sites": {}}
    config_name.write_text(json.dumps(minimal_config))

    with (
        mock_stderr_isatty(is_tty=False),
        patch("logging.basicConfig") as mock_basic_config,
        patch("myhoard.myhoard.MyHoard") as mock_hoard,
    ):
        mock_hoard_instance = MagicMock()
        mock_hoard_instance.run.return_value = 0
        mock_hoard.return_value = mock_hoard_instance

        result = main(["--config", str(config_name), "--log-level", "INFO"])

        assert result == 0
        assert mock_basic_config.call_count == 1

        call_kwargs = mock_basic_config.call_args[1]
        assert "handlers" in call_kwargs
        assert "format" not in call_kwargs
        assert call_kwargs["level"] == "INFO"

        handlers = call_kwargs["handlers"]
        assert len(handlers) == 1

        from systemd.journal import JournalHandler  # type: ignore[import-untyped]

        assert isinstance(handlers[0], JournalHandler)

        formatter = handlers[0].formatter
        assert formatter is not None
        assert "%(asctime)s" not in formatter._fmt  # pylint: disable=protected-access


def test_main_logging_uses_standard_format_when_stderr_is_tty(
    tmp_path, mock_stderr_isatty
):  # pylint: disable=redefined-outer-name
    """Verify standard logging is used when running in an interactive terminal."""
    from myhoard.myhoard import main

    config_name = tmp_path / "myhoard.json"
    minimal_config = {"backup_sites": {}}
    config_name.write_text(json.dumps(minimal_config))

    with (
        mock_stderr_isatty(is_tty=True),
        patch("logging.basicConfig") as mock_basic_config,
        patch("myhoard.myhoard.MyHoard") as mock_hoard,
    ):
        mock_hoard_instance = MagicMock()
        mock_hoard_instance.run.return_value = 0
        mock_hoard.return_value = mock_hoard_instance

        result = main(["--config", str(config_name), "--log-level", "DEBUG"])

        assert result == 0
        assert mock_basic_config.call_count == 1

        call_kwargs = mock_basic_config.call_args[1]
        assert "format" in call_kwargs
        assert "handlers" not in call_kwargs
        assert call_kwargs["level"] == "DEBUG"

        log_format = call_kwargs["format"]
        assert "%(asctime)s" in log_format


def test_main_logging_uses_standard_format_when_journal_unavailable(tmp_path):
    """Verify fallback to standard logging when systemd.journal module is not available."""
    from myhoard.myhoard import main

    config_name = tmp_path / "myhoard.json"
    minimal_config = {"backup_sites": {}}
    config_name.write_text(json.dumps(minimal_config))

    with (
        patch("myhoard.myhoard.journal", None),
        patch("logging.basicConfig") as mock_basic_config,
        patch("myhoard.myhoard.MyHoard") as mock_hoard,
    ):
        mock_hoard_instance = MagicMock()
        mock_hoard_instance.run.return_value = 0
        mock_hoard.return_value = mock_hoard_instance

        result = main(["--config", str(config_name), "--log-level", "WARNING"])

        assert result == 0
        assert mock_basic_config.call_count == 1

        call_kwargs = mock_basic_config.call_args[1]
        assert "format" in call_kwargs
        assert "handlers" not in call_kwargs
        assert call_kwargs["level"] == "WARNING"

        log_format = call_kwargs["format"]
        assert "%(asctime)s" in log_format


@pytest.mark.parametrize("log_level", ["DEBUG", "INFO", "WARNING", "ERROR"])
@pytest.mark.parametrize("is_tty", [True, False])
def test_main_logging_respects_log_level_argument(
    tmp_path, mock_stderr_isatty, log_level, is_tty
):  # pylint: disable=redefined-outer-name
    """Verify the --log-level CLI argument is properly applied in all scenarios."""
    from myhoard.myhoard import main

    config_name = tmp_path / "myhoard.json"
    minimal_config = {"backup_sites": {}}
    config_name.write_text(json.dumps(minimal_config))

    with (
        mock_stderr_isatty(is_tty=is_tty),
        patch("logging.basicConfig") as mock_basic_config,
        patch("myhoard.myhoard.MyHoard") as mock_hoard,
    ):
        mock_hoard_instance = MagicMock()
        mock_hoard_instance.run.return_value = 0
        mock_hoard.return_value = mock_hoard_instance

        result = main(["--config", str(config_name), "--log-level", log_level])

        assert result == 0
        assert mock_basic_config.call_count == 1

        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs["level"] == log_level
