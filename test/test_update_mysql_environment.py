# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
from myhoard.update_mysql_environment import EnvironmentUpdater
from pathlib import Path

import argparse
import pytest

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def update_environment(env_file: Path, *, with_bin_log: str, gtid_mode: str) -> list[str]:
    """Run the updater the way sudo does and return the variables it left in the file"""
    args = argparse.Namespace(env_file=str(env_file), with_bin_log=with_bin_log, gtid_mode=gtid_mode)
    EnvironmentUpdater(args).update()
    # An empty variable list leaves the file holding just a newline
    return [line for line in env_file.read_text().splitlines() if line]


@pytest.mark.parametrize(
    "with_bin_log,gtid_mode,expected",
    [
        (
            "false",
            "false",
            "MYSQLD_OPTS=--disable-log-bin --skip-slave-preserve-commit-order --event-scheduler=OFF --gtid-mode=OFF",
        ),
        ("false", "true", "MYSQLD_OPTS=--disable-log-bin --skip-slave-preserve-commit-order --event-scheduler=OFF"),
        ("true", "false", "MYSQLD_OPTS=--gtid-mode=OFF"),
    ],
)
def test_restore_phase_options_are_written(tmp_path: Path, with_bin_log: str, gtid_mode: str, expected: str) -> None:
    env_file = tmp_path / "mysqld.environment"

    lines = update_environment(env_file, with_bin_log=with_bin_log, gtid_mode=gtid_mode)

    assert lines == [expected]


def test_finalizing_a_restore_drops_the_variable(tmp_path: Path) -> None:
    # Removing MYSQLD_OPTS entirely is how mysqld gets back to the my.cnf settings once the restore is
    # done, most importantly with the event scheduler enabled again.
    env_file = tmp_path / "mysqld.environment"
    update_environment(env_file, with_bin_log="false", gtid_mode="true")

    lines = update_environment(env_file, with_bin_log="true", gtid_mode="true")

    assert lines == []


def test_unrelated_variables_are_preserved(tmp_path: Path) -> None:
    env_file = tmp_path / "mysqld.environment"
    env_file.write_text("SOME_OTHER_VAR=value\nMYSQLD_OPTS=--stale-option\n")

    lines = update_environment(env_file, with_bin_log="false", gtid_mode="true")

    assert lines == [
        "SOME_OTHER_VAR=value",
        "MYSQLD_OPTS=--disable-log-bin --skip-slave-preserve-commit-order --event-scheduler=OFF",
    ]
