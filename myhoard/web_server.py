# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from aiohttp import web
from aiohttp.web_response import json_response
from datetime import datetime, timezone
from myhoard.backup_stream import BackupStream
from myhoard.controller import Controller
from myhoard.errors import BadRequest

import asyncio
import contextlib
import enum
import json
import logging
import time
import uuid


class WebServer:
    """Provide an API to list available backups, request state changes, observe current state and obtain metrics"""

    @enum.unique
    class BackupType(str, enum.Enum):
        basebackup = "basebackup"
        binlog = "binlog"

    def __init__(self, *, controller, http_address="127.0.0.1", http_port, stats):
        super().__init__()
        self.app = web.Application()
        logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
        self.controller = controller
        self.http_address = http_address
        self.http_port = http_port
        self.log = logging.getLogger(self.__class__.__name__)
        self.runner = None
        self.site = None
        self.stats = stats
        self._add_routes()

    async def backup_create(self, request):
        """Creates new basebackup or ensures latest binlog is backed up depending on parameters"""
        with self._handle_request(name="backup_create"):
            body = await self._get_request_json(request)
            log_index = None
            backup_type = body.get("backup_type")
            wait_for_upload = body.get("wait_for_upload")
            with self.controller.lock:
                if backup_type == self.BackupType.basebackup:
                    if wait_for_upload:
                        raise BadRequest("wait_for_upload currently not supported for basebackup")
                    self.controller.mark_backup_requested(backup_reason=BackupStream.BackupReason.requested)
                elif backup_type == self.BackupType.binlog:
                    log_index = self.controller.rotate_and_back_up_binlog()
                else:
                    raise BadRequest("`backup_type` must be set to `basebackup` or `binlog` in request body")

            if log_index is not None and wait_for_upload:
                self.log.info("Waiting up to %.1f seconds for upload of %r to complete", wait_for_upload, log_index)
                start = time.monotonic()
                while True:
                    if self.controller.is_log_backed_up(log_index=log_index):
                        self.log.info("Log %r was backed up in %.1f seconds", log_index, time.monotonic() - start)
                        break
                    elapsed = time.monotonic() - start
                    if elapsed > wait_for_upload:
                        self.log.info("Log %r was not backed up in %.1f seconds", log_index, elapsed)
                        break
                    wait_time = min(wait_for_upload - elapsed, 0.1)
                    await asyncio.sleep(wait_time)

            return json_response({"success": True})

    async def backup_list(self, _request):
        with self._handle_request(name="backup_list"):
            response = {
                "backups": None,
            }
            with self.controller.lock:
                if self.controller.state["backups_fetched_at"]:
                    response["backups"] = self.controller.state["backups"]
                return json_response(response)

    async def backup_preserve(self, request):
        with self._handle_request(name="backup_preserve"):
            stream_id = request.match_info["stream_id"]
            body = await self._get_request_json(request)
            preserve_until = body.get("preserve_until")
            remove_after_restore = body.get("remove_after_restore", False)

            if preserve_until is not None:
                try:
                    preserve_until = datetime.fromisoformat(preserve_until)
                    if preserve_until.tzinfo != timezone.utc:
                        raise BadRequest("`preserve_until` must be in UTC timezone.")

                    now = datetime.now(timezone.utc)
                    if preserve_until < now:
                        raise BadRequest("`preserve_until` must be a date in the future.")
                except ValueError:
                    raise BadRequest("`preserve_until` must be a valid isoformat datetime string.")

            self.controller.mark_backup_preservation(
                stream_id=stream_id,
                preserve_until=preserve_until,
                remove_after_restore=remove_after_restore,
            )
            wait_for_applied_preservation = body.get("wait_for_applied_preservation")
            if wait_for_applied_preservation:
                self.log.info(
                    "Waiting up to %.1f seconds for preservation of backup %s to be applied.",
                    wait_for_applied_preservation,
                    stream_id,
                )
                start = time.monotonic()
                while True:
                    backup = self.controller.get_backup_by_stream_id(stream_id)
                    # the backup was or will be removed before preservation could be applied
                    if not backup or stream_id == self.controller.state["stream_to_be_purged"]:
                        if preserve_until:
                            return json_response({"success": False})
                        # preservation was removed on time
                        return json_response({"success": True})

                    backup_preserve_info = backup.get("preserve_info", {})
                    if (backup_preserve_info.get("preserve_until") is None and preserve_until is None) or (
                        backup_preserve_info.get("preserve_until") == preserve_until.isoformat()
                    ):
                        self.log.info("Preservation for backup %s was applied.", stream_id)
                        break

                    elapsed = time.monotonic() - start
                    if elapsed > wait_for_applied_preservation:
                        self.log.info(
                            "Preservation for backup %s was not applied up in %.1f seconds",
                            stream_id,
                            elapsed,
                        )
                        # waiting time was exceeded
                        return json_response({"success": False, "preservation_is_still_pending": True})

                    wait_time = min(wait_for_applied_preservation - elapsed, 0.1)
                    await asyncio.sleep(wait_time)

            return json_response({"success": True})

    async def replication_state_set(self, request):
        with self._handle_request(name="replication_state_set"):
            state = await self._get_request_json(request)
            self.validate_replication_state(state)
            self.controller.state_manager.update_state(replication_state=state)
            return json_response(state)

    async def restore_status_show(self, _request):
        with self._handle_request(name="restore_status_show"):
            if self.controller.mode != Controller.Mode.restore:
                raise BadRequest(f"Mode is {self.controller.mode}, restore status is not available")

            # If restore was just requested or our state was reloaded there might not have
            # been time to create the restore coordinator so wait a bit for that to become
            # available
            start_time = time.monotonic()
            coordinator = self.controller.restore_coordinator
            while time.monotonic() - start_time < 2 and not coordinator:
                await asyncio.sleep(0.05)
                coordinator = self.controller.restore_coordinator

            if not coordinator:
                if self.controller.mode != Controller.Mode.restore:
                    raise BadRequest(f"Mode is {self.controller.mode}, restore status is not available")
                raise Exception("Restore coordinator is not available even though state is 'restore'")

            with coordinator.state_manager.lock:
                response = {
                    "basebackup_compressed_bytes_downloaded": coordinator.basebackup_bytes_downloaded,
                    "basebackup_compressed_bytes_total": coordinator.basebackup_bytes_total,
                    "binlogs_being_restored": coordinator.binlogs_being_restored,
                    "binlogs_pending": coordinator.binlogs_pending,
                    "binlogs_restored": coordinator.binlogs_restored,
                    "phase": coordinator.phase,
                }
            return json_response(response)

    async def status_show(self, _request):
        with self._handle_request(name="status_show"):
            return json_response({"mode": self.controller.mode})

    async def status_update(self, request):
        with self._handle_request(name="status_update"):
            body = await self._get_request_json(request)
            body_mode = body.get("mode")
            if body_mode == Controller.Mode.active:
                force = body.get("force")
                if not isinstance(force, bool):
                    force = False
                if force:
                    self.log.info("Switch to active mode with force flag requested")
                self.controller.switch_to_active_mode(force=force)
            elif body_mode == Controller.Mode.observe:
                self.controller.switch_to_observe_mode()
            elif body_mode == Controller.Mode.restore:
                if not isinstance(body.get("rebuild_tables"), (bool, type(None))):
                    raise BadRequest("Field 'rebuild_tables' must be a boolean when present")
                for key in ["site", "stream_id"]:
                    if not isinstance(body.get(key), str):
                        raise BadRequest(f"Field {key!r} must be given and a string")
                if not isinstance(body.get("target_time"), (int, type(None))):
                    raise BadRequest("Field 'target_time' must be an integer when present")
                if not isinstance(body.get("target_time_approximate_ok"), (bool, type(None))):
                    raise BadRequest("Field 'target_time_approximate_ok' must be a boolean when present")
                self.controller.restore_backup(
                    rebuild_tables=False if body.get("rebuild_tables") is None else body.get("rebuild_tables"),
                    site=body["site"],
                    stream_id=body["stream_id"],
                    target_time=body.get("target_time"),
                    target_time_approximate_ok=body.get("target_time_approximate_ok"),
                )
            else:
                raise BadRequest(f"Unexpected value {body_mode!r} for field 'mode'")

            return json_response({"mode": self.controller.mode})

    @contextlib.contextmanager
    def _handle_request(self, *, name):
        with self._convert_exception_to_bad_request(method_name=name):
            with self.stats.timing_manager(f"myhoard.http.{name}"):
                yield

    @contextlib.contextmanager
    def _convert_exception_to_bad_request(self, *, method_name):
        try:
            yield
        except (BadRequest, ValueError) as ex:
            raise web.HTTPBadRequest(content_type="application/json", text=json.dumps({"message": str(ex)}))
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Exception while handling request %r", method_name)
            self.stats.unexpected_exception(ex=ex, where=method_name)
            raise web.HTTPInternalServerError(content_type="application/json", text=json.dumps({"message": str(ex)}))

    async def _get_request_json(self, request):
        try:
            body = json.loads(await request.text())
        except Exception as ex:  # pylint= disable=broad-except
            raise BadRequest(f"Failed to deserialize request body as JSON: {str(ex)}")
        if not isinstance(body, dict):
            raise BadRequest("Request body must be JSON object")
        return body

    async def start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.http_address, self.http_port)
        await self.site.start()
        self.log.info("Web server running")

    async def stop(self):
        if not self.site:
            return
        assert self.runner is not None
        self.log.info("Stopping web server")
        await self.runner.cleanup()
        self.log.info("Web server stopped")
        self.site = None

    def _add_routes(self):
        self.app.add_routes(
            [
                web.get("/backup", self.backup_list),
                web.post("/backup", self.backup_create),
                web.put("/backup/{stream_id}/preserve", self.backup_preserve),
                web.put("/replication_state", self.replication_state_set),
                web.get("/status", self.status_show),
                web.put("/status", self.status_update),
                web.get("/status/restore", self.restore_status_show),
            ]
        )

    @classmethod
    def validate_replication_state(cls, state):
        """Validates that given state value matches the format returned by parse_gtid_range_string"""
        if not isinstance(state, dict):
            raise BadRequest("Replication state must be name => object mapping")
        for gtids in state.values():
            if not isinstance(gtids, dict):
                raise BadRequest("Replication state objects must be uuid => object mappings")
            for maybe_uuid, ranges in gtids.items():
                try:
                    uuid.UUID(maybe_uuid)
                except Exception:  # pylint: disable=broad-except
                    raise BadRequest("Replication state objects must be uuid => object mappings")
                if not isinstance(ranges, list):
                    raise BadRequest("Individual values must be uuid => [[start1, end1], ...] mappings")
                for rng in ranges:
                    if not isinstance(rng, list) or len(rng) != 2:
                        raise BadRequest("List entries must be 2 element ([start, end]) lists")
                    for start_end in rng:
                        if not isinstance(start_end, int):
                            raise BadRequest("Range start/end values must be integers")
