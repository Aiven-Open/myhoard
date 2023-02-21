# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
"""
myhoard - statsd

Supports Telegraf's statsd protocol extension for 'key=value' tags:

  https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd
"""
from contextlib import contextmanager
from copy import copy
from types import ModuleType
from typing import Any, Dict, Optional, Union

import datetime
import enum
import logging
import os
import socket
import time


class StatsClient:
    def __init__(
        self,
        *,
        host: Optional[str],
        port: int = 8125,
        sentry_dsn: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        self.log = logging.getLogger("StatsClient")

        tags = tags or {}
        sentry_tags = copy(tags)
        self.sentry_config: Dict[str, Any] = {
            "dsn": sentry_dsn or None,
            "hostname": os.environ.get("HOSTNAME") or None,
            "tags": sentry_tags,
            "ignore_exceptions": [],
        }

        self.sentry: Optional[ModuleType] = None
        self._initialize_sentry()

        self._dest_addr = (host, port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tags = tags

    @contextmanager
    def timing_manager(self, metric, tags=None):
        start_time = time.monotonic()
        tags = (tags or {}).copy()
        try:
            yield
        except:  # noqa pylint: disable=broad-except,bare-except
            tags["success"] = "0"
            self.timing(metric, time.monotonic() - start_time, tags)
            raise
        else:
            tags["success"] = "1"
            self.timing(metric, time.monotonic() - start_time, tags)

    def update_sentry_config(self, config):
        new_config = self.sentry_config.copy()
        new_config.update(config)
        if new_config == self.sentry_config:
            return

        self.sentry_config = new_config
        self._initialize_sentry()

    def gauge_timedelta(self, metric: str, value: datetime.timedelta, *, tags=None) -> None:
        self._send(metric, b"g", value.total_seconds(), tags)

    def gauge_float(self, metric: str, value: Union[float, int], *, tags=None) -> None:
        self._send(metric, b"g", float(value), tags)

    def gauge_int(self, metric: str, value: int, *, tags=None) -> None:
        if not isinstance(value, int):
            raise ValueError(f"Invalid int value for {metric}: {value!r}")
        self._send(metric, b"g", int(value), tags)

    def increase(self, metric: str, inc_value: int = 1, tags=None) -> None:
        self._send(metric, b"c", inc_value, tags)

    def timing(self, metric: str, value: Union[float, int, datetime.timedelta], tags=None) -> None:
        if isinstance(value, datetime.timedelta):
            value = value.total_seconds()
        value = float(value)
        self._send(metric, b"ms", value, tags)

    def unexpected_exception(self, *, ex, where, tags=None, elapsed=None):
        all_tags = {
            "exception": ex.__class__.__name__,
            "where": where,
        }
        all_tags.update(tags or {})
        self.increase("exception", tags=all_tags)

        if not self.sentry:
            return

        sentry_tags = {**(tags or {}), "where": where}

        with self.sentry.push_scope() as scope:
            for key, value in sentry_tags.items():
                scope.set_tag(key, value)

            if elapsed:
                scope.set_extra("time_spent", elapsed)

            if getattr(ex, "sentry_fingerprint", None):
                # "{{ default }}" is a special tag sentry replaces with default fingerprint.
                # Only set sentry_fingerprint if you are sure automatic grouping in Sentry is failing. Don't add items
                # like service_id, unless there is a very good reason to have separate Sentry issues for each service.
                scope.fingerprint = ["{{ default }}", ex.sentry_fingerprint]

            self.sentry.capture_exception(ex)

    def _initialize_sentry(self) -> None:
        if not self.sentry_config.get("dsn"):
            self.sentry = None
            return

        try:
            from sentry_sdk.integrations.logging import LoggingIntegration

            import sentry_sdk

            sentry_logging = LoggingIntegration(
                level=logging.INFO,
                event_level=logging.CRITICAL,
            )

            sentry_sdk.init(dsn=self.sentry_config["dsn"], integrations=[sentry_logging])
            with sentry_sdk.configure_scope() as scope:
                scope.set_extra("hostname", self.sentry_config.get("hostname"))
                for key, value in self.sentry_config.get("tags", {}).items():
                    scope.set_tag(key, value)

            self.sentry = sentry_sdk
        except ImportError:
            self.sentry = None
            self.log.warning("Cannot enable Sentry.io sending: importing 'sentry_sdk' failed")

    def _send(self, metric: str, metric_type, value, tags):
        try:
            # format: "user.logins,service=payroll,region=us-west:1|c"
            parts = [metric.encode("utf-8"), b":", str(value).encode("utf-8"), b"|", metric_type]
            send_tags = self.tags.copy()
            send_tags.update(tags or {})
            for tag, tag_value in sorted(send_tags.items()):
                if isinstance(tag_value, enum.Enum):
                    tag_value = tag_value.value
                if tag_value is None:
                    tag_value = ""
                elif isinstance(tag_value, datetime.datetime):
                    if tag_value.tzinfo:
                        tag_value = tag_value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                    tag_value = tag_value.isoformat()[:19].replace("-", "").replace(":", "") + "Z"
                elif isinstance(tag_value, datetime.timedelta):
                    tag_value = f"{int(tag_value.total_seconds())}s"
                elif not isinstance(tag_value, str):
                    tag_value = str(tag_value)
                if " " in tag_value or ":" in tag_value or "|" in tag_value or "=" in tag_value:
                    tag_value = "INVALID"
                parts.insert(1, f",{tag}={tag_value}".encode("utf-8"))

            if None not in self._dest_addr:
                self._socket.sendto(b"".join(parts), self._dest_addr)
        except Exception:  # pylint: disable=broad-except,bare-except
            self.log.exception(
                "Unexpected exception in statsd send: metric=%r, metric_type=%r, value=%r, tags=%r, _dest_addr=%r",
                metric,
                metric_type,
                value,
                tags,
                self._dest_addr,
            )

    def close(self):
        self._socket.close()
