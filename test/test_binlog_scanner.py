# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import json
import os

import pytest

import myhoard.util as myhoard_util
from myhoard.binlog_scanner import BinlogScanner

from . import build_statsd_client

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def test_read_gtids_from_log(session_tmpdir, mysql_master):
    state_file_name = os.path.join(session_tmpdir().strpath, "scanner_state.json")
    scanner = BinlogScanner(
        binlog_prefix=mysql_master.config_options.binlog_file_prefix,
        server_id=mysql_master.server_id,
        state_file=state_file_name,
        stats=build_statsd_client(),
    )
    with open(state_file_name, "r") as f:
        assert json.load(f) == scanner.state

    scanner.scan_new(None)
    scanner.scan_removed(None)
    # We don't use binlog when initializing so there are no binlogs for fresh server
    assert len(scanner.binlogs) == 0
    with open(state_file_name, "r") as f:
        assert json.load(f) == scanner.state

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("CREATE TABLE foo(id INTEGER PRIMARY KEY)")
        cursor.execute("COMMIT")
        cursor.execute("INSERT INTO foo (id) VALUES (1)")
        cursor.execute("COMMIT")
        cursor.execute("FLUSH BINARY LOGS")
        cursor.execute("SHOW MASTER STATUS")
        master_info = cursor.fetchone()

    scanner.scan_new(None)
    scanner.scan_removed(None)
    with open(state_file_name, "r") as f:
        assert json.load(f) == scanner.state
    assert len(scanner.binlogs) == 1
    print(master_info)
    binlog1 = scanner.binlogs[0]
    assert binlog1["file_name"] == "bin.000001"
    assert len(binlog1["gtid_ranges"]) == 1
    server_uuid, ranges = master_info["Executed_Gtid_Set"].split(":")
    range_start, range_end = ranges.split("-")
    range1 = binlog1["gtid_ranges"][0]
    assert range1["server_uuid"] == server_uuid
    assert range1["start"] == int(range_start)
    assert range1["end"] == int(range_end)
    assert range1["server_id"] == mysql_master.config_options.server_id

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("CREATE TABLE foo2(id INTEGER PRIMARY KEY)")
        cursor.execute("COMMIT")
        cursor.execute("FLUSH BINARY LOGS")
        cursor.execute("PURGE BINARY LOGS TO 'bin.000002'")
        cursor.execute("SHOW MASTER STATUS")
        master_info = cursor.fetchone()

    scanner.scan_new(None)
    scanner.scan_removed(None)
    with open(state_file_name, "r") as f:
        scanner_state = json.load(f)
        assert scanner_state == scanner.state
    assert len(scanner.binlogs) == 1
    binlog2 = scanner.binlogs[0]
    assert binlog2["file_name"] == "bin.000002"
    assert len(binlog2["gtid_ranges"]) == 1
    assert binlog2["gtid_ranges"][0]["start"] == range1["end"] + 1
    expected_end = int(master_info["Executed_Gtid_Set"].split("-")[-1])
    assert binlog2["gtid_ranges"][0]["end"] == expected_end

    scanner.scan_new(None)
    scanner.scan_removed(None)
    assert scanner.state == scanner_state

    with myhoard_util.mysql_cursor(**mysql_master.connect_options) as cursor:
        cursor.execute("FLUSH BINARY LOGS")

    scanner.scan_new(None)
    scanner.scan_removed(None)
    assert len(scanner.binlogs) == 2
    assert scanner.binlogs[0] == binlog2
    binlog4 = scanner.binlogs[1]
    assert binlog4["file_name"] == "bin.000003"
    assert len(binlog4["gtid_ranges"]) == 0

    scanner = BinlogScanner(
        binlog_prefix=mysql_master.config_options.binlog_file_prefix,
        server_id=mysql_master.server_id,
        state_file=state_file_name,
        stats=build_statsd_client(),
    )
    assert len(scanner.binlogs) == 2
    assert scanner.binlogs[0] == binlog2
    binlog4 = scanner.binlogs[1]
    assert binlog4["file_name"] == "bin.000003"
