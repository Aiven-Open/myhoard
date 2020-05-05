# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import os

import pytest
from myhoard.append_only_state_manager import AppendOnlyStateManager

pytestmark = [pytest.mark.unittest, pytest.mark.all]


def test_basic_operations(session_tmpdir):
    state_file_name = os.path.join(session_tmpdir().strpath, "aosm.txt")
    entries = []
    aosm = AppendOnlyStateManager(entries=entries, state_file=state_file_name)
    assert aosm.entries == []
    file_size = os.stat(state_file_name).st_size
    assert file_size == 0

    aosm.append({"foo": "bar0"})
    assert entries == [{"foo": "bar0"}]
    for index in range(1010):
        new_file_size = os.stat(state_file_name).st_size
        assert new_file_size > file_size
        file_size = new_file_size
        aosm.append({"foo": f"bar{index + 1}"})
        assert len(entries) == index + 2

    for index in range(1000):
        new_file_size = os.stat(state_file_name).st_size
        # File size keeps on growing even though data is being deleted because only deletion markers are being written
        assert new_file_size > file_size
        file_size = new_file_size
        aosm.remove_head()
        assert len(entries) == 1010 - index

    new_file_size = os.stat(state_file_name).st_size
    assert new_file_size > file_size
    file_size = new_file_size

    entries2 = []
    AppendOnlyStateManager(entries=entries2, state_file=state_file_name)

    assert set(entry["foo"] for entry in entries) == {f"bar{index}" for index in range(1000, 1011)}
    assert set(entry["foo"] for entry in entries2) == {f"bar{index}" for index in range(1000, 1011)}

    # This deletion takes us over the maximum number of deleted entries to keep around and file is rewritten
    aosm.remove_head()
    assert set(entry["foo"] for entry in entries) == {f"bar{index}" for index in range(1001, 1011)}
    new_file_size = os.stat(state_file_name).st_size
    assert new_file_size < file_size
    file_size = new_file_size

    entries2 = []
    AppendOnlyStateManager(entries=entries2, state_file=state_file_name)
    assert set(entry["foo"] for entry in entries2) == {f"bar{index}" for index in range(1001, 1011)}

    aosm.remove_head()
    assert set(entry["foo"] for entry in entries) == {f"bar{index}" for index in range(1002, 1011)}
    new_file_size = os.stat(state_file_name).st_size
    assert new_file_size > file_size
    file_size = new_file_size

    aosm.append_many([{"foo": "bar1011"}, {"foo": "bar1012"}])
    assert set(entry["foo"] for entry in entries) == {f"bar{index}" for index in range(1002, 1013)}
    new_file_size = os.stat(state_file_name).st_size
    assert new_file_size > file_size
    file_size = new_file_size

    entries2 = []
    AppendOnlyStateManager(entries=entries2, state_file=state_file_name)
    assert set(entry["foo"] for entry in entries2) == {f"bar{index}" for index in range(1002, 1013)}

    aosm.remove_many_from_head(3)
    assert set(entry["foo"] for entry in entries) == {f"bar{index}" for index in range(1005, 1013)}
    new_file_size = os.stat(state_file_name).st_size
    assert new_file_size > file_size

    entries2 = []
    AppendOnlyStateManager(entries=entries2, state_file=state_file_name)
    assert set(entry["foo"] for entry in entries2) == {f"bar{index}" for index in range(1005, 1013)}
