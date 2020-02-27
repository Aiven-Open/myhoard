# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import copy
import os
import subprocess
from datetime import datetime

import myhoard.util as myhoard_util
import pytest

from . import generate_rsa_key_pair

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def test_read_gtids_from_log():
    fn = os.path.join(os.path.dirname(__file__), "binlog")
    events = [(datetime.utcfromtimestamp(event[0]).isoformat(), event[1], event[2], event[3], event[4])
              for event in myhoard_util.read_gtids_from_log(fn)]

    server_uuid = "c1100de1-04f7-11e9-82fd-60f6773756fe"
    server_id = 1
    expected_events = [
        ("2019-01-03T08:31:29", server_id, server_uuid, 1000003, 195),
        ("2019-01-03T08:31:33", server_id, server_uuid, 1000004, 495),
        ("2019-01-03T08:31:37", server_id, server_uuid, 1000005, 795),
        ("2019-01-03T08:31:41", server_id, server_uuid, 1000006, 1095),
        ("2019-01-03T08:31:51", server_id, server_uuid, 1000007, 1395),
        ("2019-01-03T08:32:08", server_id, server_uuid, 1000008, 1695),
        ("2019-01-03T08:32:15", server_id, server_uuid, 1000009, 2027),
        ("2019-01-03T08:32:19", server_id, server_uuid, 1000010, 2327),
    ]
    assert events == expected_events

    ranges = list(myhoard_util.build_gtid_ranges(myhoard_util.read_gtids_from_log(fn)))
    expected_ranges = [
        {
            "end": 1000010,
            "end_ts": 1546504339,
            "server_id": server_id,
            "server_uuid": server_uuid,
            "start": 1000003,
            "start_ts": 1546504289,
        },
    ]
    assert ranges == expected_ranges

    with pytest.raises(ValueError):
        for _ in myhoard_util.read_gtids_from_log(__file__):
            pass

    events = [(datetime.utcfromtimestamp(event[0]).isoformat(), event[1], event[2], event[3], event[4])
              for event in myhoard_util.read_gtids_from_log(fn, read_until_time=1546504335)]
    expected_events.pop()
    expected_events.pop()
    assert events == expected_events

    events = [(datetime.utcfromtimestamp(event[0]).isoformat(), event[1], event[2], event[3], event[4])
              for event in myhoard_util.read_gtids_from_log(fn, read_until_position=1695)]
    expected_events.pop()
    assert events == expected_events


def test_build_gtid_ranges():
    events = [
        (1000, 1, "a", 1, 100),
        (1001, 1, "a", 2, 200),
        (1002, 1, "a", 4, 300),
        (1003, 2, "b", 5, 400),
        (1004, 2, "b", 6, 500),
    ]
    ranges = list(myhoard_util.build_gtid_ranges(events))
    expected_ranges = [
        {
            "end": 2,
            "end_ts": 1001,
            "server_id": 1,
            "server_uuid": "a",
            "start": 1,
            "start_ts": 1000,
        },
        {
            "end": 4,
            "end_ts": 1002,
            "server_id": 1,
            "server_uuid": "a",
            "start": 4,
            "start_ts": 1002,
        },
        {
            "end": 6,
            "end_ts": 1004,
            "server_id": 2,
            "server_uuid": "b",
            "start": 5,
            "start_ts": 1003,
        },
    ]
    assert ranges == expected_ranges


def test_partition_sort_and_combine_gtid_ranges():
    ranges = [
        {
            "server_uuid": "uuid1",
            "start": 1,
            "end": 3
        },
        {
            "server_uuid": "uuid1",
            "start": 6,
            "end": 7
        },
        {
            "server_uuid": "uuid1",
            "start": 8,
            "end": 8
        },
        {
            "server_uuid": "uuid2",
            "start": 10,
            "end": 12
        },
        {
            "server_uuid": "uuid2",
            "start": 4,
            "end": 9
        },
        {
            "server_uuid": "uuid1",
            "start": 2,
            "end": 2
        },
        {
            "server_uuid": "uuid1",
            "start": 2,
            "end": 4
        },
    ]
    result = myhoard_util.partition_sort_and_combine_gtid_ranges(ranges)
    assert result == {"uuid1": [[1, 4], [6, 8]], "uuid2": [[4, 12]]}


def test_first_contains_gtids_not_in_second():
    first = [
        {
            "server_uuid": "uuid1",
            "start": 1,
            "end": 3
        },
        {
            "server_uuid": "uuid1",
            "start": 6,
            "end": 7
        },
        {
            "server_uuid": "uuid1",
            "start": 8,
            "end": 8
        },
        {
            "server_uuid": "uuid2",
            "start": 10,
            "end": 12
        },
        {
            "server_uuid": "uuid2",
            "start": 4,
            "end": 9
        },
        {
            "server_uuid": "uuid1",
            "start": 2,
            "end": 2
        },
        {
            "server_uuid": "uuid1",
            "start": 2,
            "end": 4
        },
    ]
    second = copy.deepcopy(first)
    assert not myhoard_util.first_contains_gtids_not_in_second(first, second)
    second.append({"server_uuid": "uuid3", "start": 1, "end": 1})
    assert not myhoard_util.first_contains_gtids_not_in_second(first, second)
    first.append({"server_uuid": "uuid4", "start": 1, "end": 1})
    assert myhoard_util.first_contains_gtids_not_in_second(first, second)
    first.pop()
    first[0]["end"] = 11
    assert myhoard_util.first_contains_gtids_not_in_second(first, second)
    second.append({"server_uuid": "uuid1", "start": 1, "end": 12})
    assert not myhoard_util.first_contains_gtids_not_in_second(first, second)


def test_parse_gtid_executed_and_truncate_gtid_executed():
    gtid_executed_str = "uuid1:1-6:9:12-20,uuid2:1-30"
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9], [12, 20]], "uuid2": [[1, 30]]}
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:15")
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9], [12, 15]], "uuid2": [[1, 30]]}
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:10")
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9]], "uuid2": [[1, 30]]}
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:9")
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9]], "uuid2": [[1, 30]]}
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:9, uuid2:7")
    assert gtid_executed == {"uuid1": [[1, 6], [9, 9]], "uuid2": [[1, 7]]}
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid1:8")
    assert gtid_executed == {"uuid1": [[1, 6]], "uuid2": [[1, 30]]}
    myhoard_util.truncate_gtid_executed(gtid_executed, "uuid2:1")
    assert gtid_executed == {"uuid1": [[1, 6]], "uuid2": [[1, 1]]}


def test_are_gtids_in_executed_set():
    gtid_executed_str = "uuid1:1-6:9:12-20,uuid2:1-30"
    gtid_executed = myhoard_util.parse_gtid_range_string(gtid_executed_str)
    assert myhoard_util.are_gtids_in_executed_set(
        gtid_executed, [{
            "server_uuid": "uuid1",
            "start": 18,
            "end": 18,
        }, {
            "server_uuid": "uuid2",
            "start": 30,
            "end": 30,
        }]
    )
    assert not myhoard_util.are_gtids_in_executed_set(gtid_executed, [{
        "server_uuid": "uuid1",
        "start": 18,
        "end": 21,
    }])
    assert myhoard_util.are_gtids_in_executed_set(
        gtid_executed, [{
            "server_uuid": "uuid1",
            "start": 18,
            "end": 21,
        }], exclude_uuid="uuid1"
    )
    assert not myhoard_util.are_gtids_in_executed_set(gtid_executed, [{
        "server_uuid": "uuid3",
        "start": 1,
        "end": 1,
    }])


def test_encrypt_decrypt():
    private_key_pem, public_key_pem = generate_rsa_key_pair()
    for size in range(100):
        data = os.urandom(size)
        encrypted = myhoard_util.rsa_encrypt_bytes(public_key_pem, data)
        decrypted = myhoard_util.rsa_decrypt_bytes(private_key_pem, encrypted)
        assert data != encrypted
        assert data == decrypted


def test_detect_running_process_id():
    pytest_id = myhoard_util.detect_running_process_id("python3 -m pytest")
    coverage_id = myhoard_util.detect_running_process_id("python3 -m coverage")
    if pytest_id is None and coverage_id is None:
        output = subprocess.check_output(["ps", "-x", "--cols", "1000", "-o", "pid,command"])
        raise AssertionError(f"None of the commands included in ps output {output!r}")
    assert myhoard_util.detect_running_process_id("certainlynosuchprocesscurrentlyrunning") is None
