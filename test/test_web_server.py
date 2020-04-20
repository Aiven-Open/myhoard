# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import uuid

import pytest
from myhoard.backup_stream import BackupStream
from myhoard.controller import Controller
from myhoard.errors import BadRequest
from myhoard.restore_coordinator import RestoreCoordinator
from myhoard.web_server import WebServer

from . import awhile_asserts, while_asserts

pytestmark = [pytest.mark.unittest, pytest.mark.all]


async def test_backup_create(master_controller, web_client):
    controller = master_controller[0]

    controller.switch_to_active_mode()
    controller.start()

    def is_streaming_binlogs():
        assert controller.backup_streams
        assert controller.backup_streams[0].active_phase == BackupStream.ActivePhase.binlog

    while_asserts(is_streaming_binlogs, timeout=15)

    log_count_before = len(controller.backup_streams[0].remote_binlogs)
    await post_and_verify_json_body(
        web_client, "/backup", {
            "backup_type": WebServer.BackupType.binlog,
            "wait_for_upload": 1
        }
    )
    log_count_after = len(controller.backup_streams[0].remote_binlogs)
    assert log_count_after > log_count_before

    await post_and_verify_json_body(web_client, "/backup", {"backup_type": WebServer.BackupType.binlog})

    await post_and_verify_json_body(web_client, "/backup", {}, expected_status=400)

    await post_and_verify_json_body(web_client, "/backup", {"backup_type": WebServer.BackupType.basebackup})

    async def has_two_backups():
        response = await get_and_verify_json_body(web_client, "/backup")
        assert response["backups"]
        assert len(response["backups"]) == 2

    await awhile_asserts(has_two_backups, timeout=15)


async def test_backup_list(master_controller, web_client):
    controller = master_controller[0]
    response = await get_and_verify_json_body(web_client, "/backup")
    # backups is None when backend hasn't listed backups from file storage yet
    assert response == {"backups": None}

    async def backup_list_not_none():
        assert (await get_and_verify_json_body(web_client, "/backup"))["backups"] is not None

    controller.start()
    # Backups is empty list when backups have been listed but there are none
    await awhile_asserts(backup_list_not_none)

    def is_streaming_binlogs():
        assert controller.backup_streams
        assert controller.backup_streams[0].is_streaming_binlogs()

    # Switching to active mode causes new backup to be created, which should be returned in listing soon
    controller.switch_to_active_mode()
    while_asserts(is_streaming_binlogs, timeout=15)

    async def has_backup():
        response = await get_and_verify_json_body(web_client, "/backup")
        assert response["backups"]
        assert len(response["backups"]) == 1
        backup = response["backups"][0]
        expected = {"basebackup_info", "closed_at", "completed_at", "recovery_site", "resumable", "site", "stream_id"}
        assert set(backup) == expected

    await awhile_asserts(has_backup)


async def test_replication_state_set(master_controller, web_client):
    controller = master_controller[0]
    state = {
        "server-1": {
            "eff55bc8-dec8-45f6-bf9f-149228c08671": [[1, 4], [7, 89]],
        }
    }
    response = await put_and_verify_json_body(web_client, "/replication_state", state)
    assert response == state
    assert controller.state["replication_state"] == state
    await put_and_verify_json_body(web_client, "/replication_state", {"foo": "bar"}, expected_status=400)


async def test_status_show(master_controller, web_client):
    controller = master_controller[0]
    response = await get_and_verify_json_body(web_client, "/status")
    assert response["mode"] == Controller.Mode.idle
    controller.switch_to_active_mode()
    response = await get_and_verify_json_body(web_client, "/status")
    assert response["mode"] == Controller.Mode.promote


async def test_status_update_to_active(master_controller, web_client):
    controller = master_controller[0]
    response = await put_and_verify_json_body(web_client, "/status", {"mode": "active"})
    assert response["mode"] == Controller.Mode.promote
    assert controller.mode == Controller.Mode.promote

    response = await put_and_verify_json_body(web_client, "/status", {"force": True, "mode": "active"}, expected_status=400)
    assert response["message"] == "Can only force promotion while waiting for binlogs to be applied"

    response = await put_and_verify_json_body(web_client, "/status", {"mode": Controller.Mode.observe}, expected_status=400)
    assert response["message"] == "Switch from promote to observe mode is not allowed"


async def test_status_update_to_observe(master_controller, web_client):
    controller = master_controller[0]
    response = await put_and_verify_json_body(web_client, "/status", {"mode": "observe"})
    assert response["mode"] == Controller.Mode.observe
    assert controller.mode == Controller.Mode.observe


async def test_status_update_to_restore(master_controller, web_client):
    response = await put_and_verify_json_body(
        web_client, "/status", {
            "mode": "restore",
            "site": "default",
            "stream_id": "abc"
        }, expected_status=400
    )
    assert response["message"] == "Requested backup 'abc' for site 'default' not found"

    response = await put_and_verify_json_body(
        web_client, "/status", {
            "mode": "restore",
            "site": "default"
        }, expected_status=400
    )
    assert response["message"] == "Field 'stream_id' must be given and a string"

    response = await put_and_verify_json_body(
        web_client,
        "/status",
        {
            "mode": "restore",
            "site": "default",
            "stream_id": "abc",
            "target_time": "foo"
        },
        expected_status=400,
    )
    assert response["message"] == "Field 'target_time' must be an integer when present"

    async def restore_status_returned():
        response = await get_and_verify_json_body(web_client, "/status/restore")
        assert isinstance(response["basebackup_compressed_bytes_downloaded"], int)
        assert isinstance(response["basebackup_compressed_bytes_total"], int)
        assert isinstance(response["binlogs_being_restored"], int)
        assert isinstance(response["binlogs_pending"], int)
        assert isinstance(response["binlogs_restored"], int)
        # Operation will fail because we faked the backup info
        assert response["phase"] != RestoreCoordinator.Phase.failed

    master_controller[0].state["backups"].append({
        "stream_id": "abc",
        "site": "default",
        "basebackup_info": {
            "end_ts": 1234567
        }
    })
    await put_and_verify_json_body(web_client, "/status", {"mode": "restore", "site": "default", "stream_id": "abc"})
    master_controller[0].start()
    await awhile_asserts(restore_status_returned, timeout=2)


async def get_and_verify_json_body(client, path, *, expected_status=200):
    response = await client.get(path)
    response_json = await response.json()
    assert response.status == expected_status, f"{response.status} != {expected_status}: {response_json}"
    return response_json


async def post_and_verify_json_body(client, path, body, *, expected_status=200):
    response = await client.post(path, json=body)
    response_json = await response.json()
    assert response.status == expected_status, f"{response.status} != {expected_status}: {response_json}"
    return response_json


async def put_and_verify_json_body(client, path, body, *, expected_status=200):
    response = await client.put(path, json=body)
    response_json = await response.json()
    assert response.status == expected_status, f"{response.status} != {expected_status}: {response_json}"
    return response_json


def test_validate_replication_state():
    uuid1 = str(uuid.uuid4())
    uuid2 = str(uuid.uuid1())
    WebServer.validate_replication_state({})  # No values is valid value
    with pytest.raises(BadRequest):
        WebServer.validate_replication_state("foo")
    WebServer.validate_replication_state({"foo": {}})  # Server with empty GTID set is valid
    with pytest.raises(BadRequest):
        WebServer.validate_replication_state({"foo": "bar"})
    with pytest.raises(BadRequest):
        WebServer.validate_replication_state({"foo": {"bar": "zob"}})
    with pytest.raises(BadRequest):
        WebServer.validate_replication_state({"foo": {"bar": []}})
    WebServer.validate_replication_state({"foo": {uuid1: []}})
    with pytest.raises(BadRequest):
        WebServer.validate_replication_state({"foo": {uuid1: ["abc"]}})
    with pytest.raises(BadRequest):
        WebServer.validate_replication_state({"foo": {uuid1: [["abc"]]}})
    with pytest.raises(BadRequest):
        WebServer.validate_replication_state({"foo": {uuid1: [[1]]}})
    WebServer.validate_replication_state({"foo": {uuid1: [[1, 2]]}})
    WebServer.validate_replication_state({"foo": {uuid1: [[1, 2], [3, 4]]}})
    WebServer.validate_replication_state({"foo": {uuid1: [[1, 2], [3, 4]]}, "zob": {}})
    WebServer.validate_replication_state({"foo": {uuid1: [[1, 2], [3, 4]]}, "zob": {uuid2: []}})
