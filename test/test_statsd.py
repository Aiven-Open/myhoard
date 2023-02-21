from enum import Enum
from myhoard.statsd import StatsClient
from socket import socket
from types import ModuleType
from typing import Callable, Generator
from unittest.mock import ANY, MagicMock, patch

import datetime
import pytest
import sentry_sdk
import time

FAKE_SENTRY_DSN = "https://random.ingest.sentry.io/project_id"


@pytest.fixture(name="sentry_init")
def fixture_sentry_init() -> Generator[Callable[[], ModuleType], None, None]:
    def inner(*a, **kw):
        hub = sentry_sdk.Hub.current
        client = sentry_sdk.Client(*a, **kw)
        hub.bind_client(client)
        return sentry_sdk

    with sentry_sdk.Hub(None):
        yield inner


@pytest.fixture(name="stats_client", scope="function")
def fixture_stats_client(monkeypatch, sentry_init: Callable[[], ModuleType]) -> StatsClient:
    stats_client = StatsClient(host=None, sentry_dsn=None)

    def fake_initialize() -> None:
        if not stats_client.sentry_config.get("dsn"):
            stats_client.sentry = None
        else:
            stats_client.sentry = sentry_init()

    monkeypatch.setattr(stats_client, "_initialize_sentry", fake_initialize)
    return stats_client


def test_update_sentry_config(stats_client: StatsClient) -> None:
    assert stats_client.sentry_config["dsn"] is None
    assert stats_client.sentry is None

    stats_client.update_sentry_config({"dsn": FAKE_SENTRY_DSN})
    assert stats_client.sentry_config["dsn"] == FAKE_SENTRY_DSN
    assert stats_client.sentry is not None


@patch.object(sentry_sdk.Scope, "set_tag")
@patch.object(sentry_sdk, "init")
def test_initialize_sentry(
    mocked_sentry_init: MagicMock,  # pylint: disable=unused-argument
    mocked_scope_set_tag: MagicMock,
    sentry_init: Callable[[], ModuleType],
) -> None:
    # pylint: disable=protected-access
    mocked_sentry_init.return_value = sentry_init  # noqa
    stats_client = StatsClient(host=None, sentry_dsn=None)
    assert stats_client.sentry is None

    stats_client.sentry_config["dsn"] = FAKE_SENTRY_DSN
    stats_client._initialize_sentry()
    assert stats_client.sentry is not None
    mocked_scope_set_tag.assert_not_called()

    stats_client.sentry_config["tags"] = {"abc": "123", "def": "456"}
    stats_client._initialize_sentry()
    assert mocked_scope_set_tag.call_count == 2


@patch.object(socket, "sendto")
def test_send(mocked_sendto: MagicMock) -> None:
    # pylint: disable=protected-access
    stats_client = StatsClient(host="fakehost", sentry_dsn=None)

    FakeEnum = Enum("FakeEnum", ["ONE", "TWO", "THREE"])

    tags = {
        "enum": FakeEnum.ONE,
        "datetime": datetime.datetime(2023, 2, 15, 12, 0, 0),
        "empty": None,
        "timedelta": datetime.timedelta(minutes=2),
        "string": "1234",
        "num": 12.34,
        "invalid_tag": "a=b",
    }
    stats_client._send(metric="random", metric_type=b"g", value=123, tags=tags)
    mocked_sendto.assert_called_once_with(
        b"random,timedelta=120s,string=1234,num=12.34,invalid_tag=INVALID,enum=1,empty=,datetime=20230215T120000Z:123|g",
        ("fakehost", 8125),
    )


@patch.object(sentry_sdk.Scope, "set_tag")
@patch.object(sentry_sdk, "capture_exception")
@patch.object(StatsClient, "increase")
def test_unexpected_exception(
    mocked_increase: MagicMock,
    mocked_sentry_capture_exception: MagicMock,
    mocked_scope_set_tag: MagicMock,
    stats_client: StatsClient,
) -> None:
    stats_client.update_sentry_config({"dsn": FAKE_SENTRY_DSN})

    ex = ValueError("backupstream cache error")
    stats_client.unexpected_exception(
        ex=ex,
        where="BackupStream._cache_basebackup_info",
        elapsed=1234,
    )
    mocked_increase.assert_called_once_with(
        "exception",
        tags={"exception": "ValueError", "where": "BackupStream._cache_basebackup_info"},
    )
    mocked_sentry_capture_exception.assert_called_once_with(ex)

    assert mocked_scope_set_tag.call_count == 1


@patch.object(StatsClient, "timing")
def test_timing_manager(mocked_timing: MagicMock, stats_client: StatsClient) -> None:
    with stats_client.timing_manager(metric="test"):
        time.sleep(0.1)

    mocked_timing.assert_called_once_with("test", ANY, {"success": "1"})

    with pytest.raises(ValueError, match="random error"):
        with stats_client.timing_manager(metric="test2"):
            raise ValueError("random error")

    mocked_timing.assert_called_with("test2", ANY, {"success": "0"})
