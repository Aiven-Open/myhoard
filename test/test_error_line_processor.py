from myhoard.util import get_tables_to_optimise
from unittest.mock import call, Mock

# pylint: disable=line-too-long


def test_optimise_errors():
    error_lines: list[str] = [
        "2022-11-24T01:59:46.971687-00:00 0 [ERROR] [MY-011825] [Xtrabackup] Found tables with row versions due to INSTANT ADD/DROP columns",
        "2022-11-24T01:59:46.972112-00:00 0 [ERROR] [MY-011825] [Xtrabackup] This feature is not stable and will cause backup corruption.",
        "2022-11-24T01:59:46.972355-00:00 0 [ERROR] [MY-011825] [Xtrabackup] Please check https://docs.percona.com/percona-xtrabackup/8.0/em/instant.html for more details.",
        "2022-11-24T01:59:46.972573-00:00 0 [ERROR] [MY-011825] [Xtrabackup] Tables found:",
        "2022-11-24T01:59:46.972848-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/action_events",
        "2022-11-24T01:59:46.973063-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/nova_notifications",
        "2022-11-24T01:59:46.973326-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/banners",
        "2022-11-24T01:59:46.973540-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/backend_users",
        "2022-11-24T01:59:46.973756-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/courses",
        "2022-11-24T01:59:46.974053-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/modules",
        "2022-11-24T01:59:46.974409-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/users",
        "2022-11-24T01:59:46.974650-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/lessons",
        "2022-11-24T01:59:46.974858-00:00 0 [ERROR] [MY-011825] [Xtrabackup] Please run OPTIMIZE TABLE or ALTER TABLE ALGORITHM=COPY on all listed tables to fix this issue.",
    ]
    log = Mock()
    tables_to_optimise = set(get_tables_to_optimise(error_lines, log=log))
    assert tables_to_optimise == {
        "defaultdb.action_events",
        "defaultdb.nova_notifications",
        "defaultdb.banners",
        "defaultdb.backend_users",
        "defaultdb.courses",
        "defaultdb.modules",
        "defaultdb.users",
        "defaultdb.lessons",
    }
    assert log.error.call_args_list == []


def test_optimise_errors_missing_expected_structure():
    error_lines: list[str] = [
        "2022-11-24T01:59:46.971687-00:00 0 [ERROR] [MY-011825] [Xtrabackup] Found tables with row versions due to INSTANT ADD/DROP columns",
        "2022-11-24T01:59:46.972112-00:00 0 [ERROR] [MY-011825] [Xtrabackup] This feature is not stable and will cause backup corruption.",
        "2022-11-24T01:59:46.972355-00:00 0 [ERROR] [MY-011825] [Xtrabackup] Please check https://docs.percona.com/percona-xtrabackup/8.0/em/instant.html for more details.",
        "2022-11-24T01:59:46.972848-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/action_events",
        "2022-11-24T01:59:46.973063-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/nova_notifications",
        "2022-11-24T01:59:46.973326-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/banners",
        "2022-11-24T01:59:46.973540-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/backend_users",
        "2022-11-24T01:59:46.973756-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/courses",
        "2022-11-24T01:59:46.974053-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/modules",
        "2022-11-24T01:59:46.974409-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/users",
        "2022-11-24T01:59:46.974650-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/lessons",
        "2022-11-24T01:59:46.974858-00:00 0 [ERROR] [MY-011825] [Xtrabackup] Please run OPTIMIZE TABLE or ALTER TABLE ALGORITHM=COPY on all listed tables to fix this issue.",
    ]
    log = Mock()
    tables_to_optimise = set(get_tables_to_optimise(error_lines, log=log))
    assert tables_to_optimise == set()
    assert log.error.call_args_list == [call("Unexpected error structure. Cannot identify tables to optimise.")]


def test_other_errors():
    error_lines: list[str] = [
        "2022-11-24T01:59:46.972573-00:00 0 [ERROR] [MY-011825] [Xtrabackup] Tables found:",
        "2022-11-24T01:59:46.972848-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/action_events",
        "2022-11-24T01:59:46.973063-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/nova_notifications",
        "2022-11-24T01:59:46.973326-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/banners",
        "2022-11-24T01:59:46.973540-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/backend_users",
        "2022-11-24T01:59:46.973756-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/courses",
        "2022-11-24T01:59:46.974053-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/modules",
        "2022-11-24T01:59:46.974409-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/users",
        "2022-11-24T01:59:46.974650-00:00 0 [ERROR] [MY-011825] [Xtrabackup] defaultdb/lessons",
    ]
    log = Mock()
    tables_to_optimise = set(get_tables_to_optimise(error_lines, log=log))
    assert tables_to_optimise == set()
    assert log.error.call_args_list == []
