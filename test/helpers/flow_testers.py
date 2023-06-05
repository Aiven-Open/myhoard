# Copyright (c) 2023 Aiven, Helsinki, Finland. https://aiven.io/
from __future__ import annotations

from _pytest.logging import LogCaptureFixture
from myhoard.backup_stream import BackupStream
from myhoard.controller import Controller
from test import wait_for_condition, while_asserts
from test.helpers.loggers import get_logger_name, log_duration

import logging


class ControllerFlowTester:
    """Helper class to test the flow of the Controller for a backup or restore."""

    def __init__(self, controller: Controller, global_timeout: int = 10, caplog: LogCaptureFixture | None = None) -> None:
        self.controller = controller
        self.timeout = global_timeout
        self.caplog = caplog
        self.logger = logging.getLogger(get_logger_name())

    @log_duration
    def wait_for_streaming_binlogs(self, *, timeout: int | None = None) -> None:
        timeout = self.timeout if timeout is None else timeout
        while_asserts(self._streaming_binlogs, timeout=timeout)

    @log_duration
    def wait_for_multiple_streams(self, *, timeout: int | None = None) -> None:
        timeout = self.timeout if timeout is None else timeout
        while_asserts(self._has_multiple_streams, timeout=timeout)

    @log_duration
    def wait_for_single_stream(self, *, timeout: int | None = None) -> None:
        timeout = self.timeout if timeout is None else timeout
        while_asserts(self._has_single_stream, timeout=timeout)

    @log_duration
    def wait_for_restore_complete(self, *, timeout: int | None = None) -> None:
        timeout = self.timeout if timeout is None else timeout
        wait_for_condition(self._restore_complete, timeout=timeout, description="restore complete")

    @log_duration
    def wait_for_fetched_backup(self, *, timeout: int | None = None) -> None:
        timeout = self.timeout if timeout is None else timeout
        wait_for_condition(self._has_fetched_backup, timeout=timeout, description="fetched backup")

    @log_duration
    def wait_for_disk_full_being_logged(self, *, timeout: int | None = None) -> None:
        timeout = self.timeout if timeout is None else timeout
        wait_for_condition(self._disk_full_being_logged, timeout=timeout, description="disk full being logged")

    def _streaming_binlogs(self) -> None:
        assert self.controller.backup_streams
        assert all(bs.active_phase == BackupStream.ActivePhase.binlog for bs in self.controller.backup_streams), [
            (s.name, s.active_phase) for s in self.controller.backup_streams
        ]

    def _has_multiple_streams(self) -> None:
        assert len(self.controller.backup_streams) > 1

    def _has_single_stream(self) -> None:
        assert len(self.controller.backup_streams) == 1

    def _restore_complete(self) -> bool:
        return self.controller.restore_coordinator is not None and self.controller.restore_coordinator.is_complete()

    def _has_fetched_backup(self) -> bool:
        return self.controller.state["backups_fetched_at"] != 0

    def _disk_full_being_logged(self) -> bool:
        if self.caplog is None:
            return False
        return any(
            "DiskFullError('No space left on device. Cannot complete xbstream-extract!')" in record.message
            for record in self.caplog.records
        )
