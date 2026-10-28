from .util import add_gtid_ranges_to_executed_set, file_name_for_basebackup_split, parse_fs_metadata, sort_and_filter_binlogs
from rohmu.errors import FileNotFoundFromStorageError

import logging


def inspect_binlogs(binlogs, promotions):
    log = logging.getLogger(__name__)
    valid_binlogs = []
    gaps = []
    expected_index = 1
    for binlog in sorted(binlogs, key=lambda entry: (entry["remote_index"], entry["server_id"])):
        remote_index = binlog["remote_index"]
        if not sort_and_filter_binlogs(binlogs=[binlog], last_index=remote_index - 1, log=log, promotions=promotions):
            continue
        if remote_index != expected_index:
            gaps.append({"expected_index": expected_index, "found_index": remote_index})
        valid_binlogs.append(binlog)
        expected_index = remote_index + 1
    return valid_binlogs, gaps


def inspect_backup(file_storage, backup, known_binlogs):
    prefix = f"{backup['site']}/{backup['stream_id']}"
    promotions = {}
    for entry in file_storage.list_iter(f"{prefix}/promotions"):
        promotion = parse_fs_metadata(entry["metadata"])
        previous = promotions.get(promotion["start_index"])
        if previous is None or promotion["promoted_at"] >= previous["promoted_at"]:
            promotions[promotion["start_index"]] = promotion
    if 1 not in promotions:
        raise RuntimeError(f"Missing initial promotion for {prefix}")
    servers = {index: promotion["server_id"] for index, promotion in promotions.items()}
    binlogs = []
    for bucket in file_storage.list_prefixes(f"{prefix}/binlogs"):
        binlogs.extend(parse_fs_metadata(entry["metadata"]) for entry in file_storage.list_iter(bucket))
    binlogs, gaps = inspect_binlogs(binlogs, servers)
    known_binlogs, _ = inspect_binlogs(known_binlogs, servers)
    last_index = binlogs[-1]["remote_index"] if binlogs else 0
    if known_binlogs and known_binlogs[-1]["remote_index"] > last_index:
        gaps.append(
            {
                "expected_index": last_index + 1,
                "found_index": None,
                "missing_through_index": known_binlogs[-1]["remote_index"],
            }
        )

    basebackup = backup["basebackup_info"]
    start = basebackup["end_ts"]
    latest = (
        max([start] + [gtid["end_ts"] for binlog in binlogs + known_binlogs for gtid in binlog["gtid_ranges"]])
        if binlogs or known_binlogs
        else None
    )
    end = start
    executed = basebackup["gtid_executed"]
    stop_index = gaps[0]["expected_index"] if gaps else None
    usable_binlogs = 0
    for binlog in binlogs:
        if stop_index is not None and binlog["remote_index"] >= stop_index:
            break
        for gtid in binlog["gtid_ranges"]:
            previous_end = max((high for _, high in executed.get(gtid["server_uuid"], [])), default=0)
            if gtid["start"] > previous_end + 1:
                gaps.append(
                    {
                        "remote_index": binlog["remote_index"],
                        "server_uuid": gtid["server_uuid"],
                        "expected_gtid": previous_end + 1,
                        "found_gtid": gtid["start"],
                    }
                )
                stop_index = binlog["remote_index"]
                break
            executed = add_gtid_ranges_to_executed_set(executed, [gtid])
        if stop_index == binlog["remote_index"]:
            break
        usable_binlogs += 1
        end = max([end] + [gtid["end_ts"] for gtid in binlog["gtid_ranges"]])
    if backup["closed_at"] is not None:
        end = min(end, backup["closed_at"])
    window = {"start": start, "end": end, "stream_id": backup["stream_id"]} if usable_binlogs and end >= start else None
    return window, latest, gaps


def basebackup_is_usable(backup, backups_by_id, file_storage):
    basebackup = backup["basebackup_info"]
    required = basebackup.get("required_streams") or []
    if basebackup.get("incremental") and not required:
        return False
    chain = []
    if basebackup.get("incremental"):
        for index, stream_id in enumerate(required):
            dependency = backups_by_id.get(stream_id)
            if dependency is None or bool(dependency["basebackup_info"].get("incremental")) != (index > 0):
                return False
            chain.append(dependency)
    chain.append(backup)
    for member in chain:
        if not member["completed_at"] or member["broken_at"] or not member["resumable"]:
            return False
        prefix = f"{member['site']}/{member['stream_id']}/basebackup.xbstream"
        splits = member["basebackup_info"].get("number_of_splits", 1)
        try:
            for index in range(1, splits + 1):
                file_storage.get_metadata_for_key(file_name_for_basebackup_split(prefix, index))
        except FileNotFoundFromStorageError:
            return False
    return True


def build_recovery_windows(backups, site_transfers, known_binlogs):
    log = logging.getLogger(__name__)
    sites = {}
    for site, file_storage in site_transfers.items():
        site_backups = {backup["stream_id"]: backup for backup in backups if backup["site"] == site}
        windows = []
        backup_windows = {stream_id: None for stream_id in site_backups}
        latest = None
        gaps = []
        excluded = []
        errors = []
        complete_ends = set()
        for stream_id, backup in site_backups.items():
            if not backup["completed_at"] or not backup["resumable"]:
                continue
            try:
                window, stream_latest, stream_gaps = inspect_backup(
                    file_storage, backup, known_binlogs.get((site, stream_id), [])
                )
                usable = basebackup_is_usable(backup, site_backups, file_storage)
            except Exception as ex:  # pylint: disable=broad-except
                log.warning("Failed to audit recovery window of %s/%s", site, stream_id, exc_info=True)
                errors.append({"stream_id": stream_id, "closed_at": backup["closed_at"], "error": str(ex)})
                continue
            if stream_latest is not None:
                latest = max(latest, stream_latest) if latest is not None else stream_latest
            gaps.extend(dict(gap, stream_id=stream_id) for gap in stream_gaps)
            if not usable:
                excluded.append(stream_id)
            elif window:
                windows.append(window)
                backup_windows[stream_id] = {"start": window["start"], "end": window["end"]}
                if not stream_gaps:
                    complete_ends.add(window["end"])

        merged = []
        for window in sorted(windows, key=lambda entry: (entry["start"], entry["end"])):
            if merged and window["start"] <= merged[-1]["end"]:
                merged[-1]["end"] = max(merged[-1]["end"], window["end"])
                merged[-1]["backups"].append(window)
            else:
                merged.append({"start": window["start"], "end": window["end"], "backups": [window]})
        current = merged[-1] if merged and merged[-1]["end"] == latest and latest in complete_ends else None
        # A stream that could not be audited may hold binlogs newer than the current window, unless it was
        # closed before the window ends.
        if current and any(error["closed_at"] is None or error["closed_at"] > current["end"] for error in errors):
            current = None
        sites[site] = {
            "window": current,
            "windows": merged,
            "backup_windows": backup_windows,
            "latest_binlog_timestamp": latest,
            "gaps": gaps,
            "excluded_backups": excluded,
            "errors": [{"stream_id": error["stream_id"], "error": error["error"]} for error in errors],
        }
    return sites
