# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import collections
import contextlib
import io
import json
import os
import re
import socket
import struct
import subprocess
import tempfile
import time

import pymysql
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.hashes import SHA1

DEFAULT_MYSQL_TIMEOUT = 4.0
ERR_TIMEOUT = 2013


@contextlib.contextmanager
def atomic_create_file(file_path, *, binary=False, perm=None, uidgid=None, inherit_owner_from_parent=False):
    """Open a temporary file for writing, rename to final name when done"""
    mode = "wb" if binary else "w"
    # if perm or uidgid is not given, copy it/them from existing file if file exists
    if perm is None or uidgid is None:
        try:
            st = os.stat(file_path)
        except FileNotFoundError:
            st = None
            if inherit_owner_from_parent and uidgid is None:
                parent_st = os.stat(os.path.dirname(file_path))
                uidgid = (parent_st.st_uid, parent_st.st_gid)

        if perm is None:
            perm = st.st_mode if st else 0o600
        if uidgid is None and st is not None:
            uidgid = (st.st_uid, st.st_gid)

    fd, tmp_file_path = tempfile.mkstemp(prefix=os.path.basename(file_path), dir=os.path.dirname(file_path), suffix=".tmp")
    try:
        tmp_st = os.stat(fd)
        if perm != tmp_st.st_mode:
            os.fchmod(fd, perm)

        if uidgid and uidgid != (tmp_st.st_uid, tmp_st.st_gid):
            os.fchown(fd, uidgid[0], uidgid[1])

        with os.fdopen(fd, mode) as out_file:
            yield out_file

        os.rename(tmp_file_path, file_path)
    except Exception:  # pytest: disable=broad-except
        with contextlib.suppress(Exception):
            os.unlink(tmp_file_path)
        raise


def change_master_to(*, cursor, options):
    """Constructs and executes CHANGE MASTER TO command based on given options"""
    sql = "CHANGE MASTER TO {}".format(", ".join(f"{k}={v!r}" for k, v in options.items()))
    cursor.execute(sql)


def make_fs_metadata(metadata):
    """File storage converts any non-string values into strings. We want to maintain the original
    data intact so convert to JSON string ourselves so that it can be parsed back reliably"""
    return {"json": json.dumps(metadata)}


def parse_fs_metadata(metadata):
    """Reverse for make_fs_metadata; return dict with original types from a string"""
    return json.loads(metadata["json"])


def read_gtids_from_log(logfile, *, read_until_position=None, read_until_time=None):
    """Yields all (timestamp, server_id (int), server_id (UUID str), GNO) tuples from GTID events in given log file.
    Running this for a 64 MiB binlog which has pathological data (only very small inserts) takes 2.0 seconds on an
    i7-7500U CPU @ 2.70GHz. There aren't any obvious optimizations (in Python) that make it considerably faster but
    since full files only need to be scanned once this should be acceptable overhead (also, processing the same file
    with mysqlbinlog takes almost exactly the same amount of time)."""
    header_size = 19
    gtid_event_code = 33
    # Full GTID event header is more than 25 bytes but we're only interested in the first 25 bytes
    gtid_header_size = 25

    header = bytearray(header_size)
    gtid_header = bytearray(gtid_header_size)
    position = 0

    with io.open(logfile, "rb") as stream:
        if not isinstance(stream, io.BufferedReader):
            stream = io.BufferedReader(stream)

        magic_bytes = stream.read(4)
        position += 4
        if magic_bytes != b"\xfebin":
            raise ValueError(f"Invalid magic bytes {magic_bytes!r}, {logfile!r} is not a valid MySQL log file")

        while True:
            if read_until_position and position >= read_until_position:
                return

            start_position = position
            bytes_read = stream.readinto(header)
            if not bytes_read:
                break
            if bytes_read < header_size:
                raise EOFError(f"Unexpected end of file encountered while reading event header, {logfile!r} is corrupt")
            position += bytes_read

            # There's one more uint32 and uint16 in the header but we don't need them so skip unpack for them
            timestamp, event_code, server_id, event_length = struct.unpack_from("<IBII", header)
            if read_until_time and timestamp >= read_until_time:
                return

            # Event length is supposed to include header size so zero should be impossible value yet sometimes
            # the value is zero and data that follows is valid
            if event_length == 0:
                continue

            position += event_length - header_size
            if event_code != gtid_event_code:
                stream.seek(event_length - header_size, io.SEEK_CUR)
                continue

            bytes_read = stream.readinto(gtid_header)
            if not bytes_read or bytes_read < gtid_header_size:
                raise EOFError(f"Unexpected end of file encountered while reading GTID event, {logfile!r} is corrupt")

            _flags, uuid_bytes, gno = struct.unpack("<B16sQ", gtid_header)
            stream.seek(event_length - header_size - gtid_header_size, io.SEEK_CUR)

            # str(uuid.UUID(bytes=uuid_bytes)) is slow, construct the UUID string manually
            b = uuid_bytes
            uuid_str = "-".join((b[0:4].hex(), b[4:6].hex(), b[6:8].hex(), b[8:10].hex(), b[10:16].hex()))
            yield timestamp, server_id, uuid_str, gno, start_position

    # We only want to handle complete log files but unfortunately there's no way to check whether current
    # file is complete or not; if MySQL server crashes it will create a new binlog without finalizing the
    # earlier one and that will end with whatever happened to be the last thing the server wrote there
    # before crashing.


def build_gtid_ranges(iterator):
    """Yield dicts containing most compact representation of all (timestamp, UUID, GNO) tuples
    returned by given iterator. Ranges are returned in correct order and no gaps are allowed.
    If input contains uninterrupted sequence of events from a single server only one range is
    produced."""
    current_range = {}
    for timestamp, server_id, server_uuid, gno, _file_position in iterator:
        if current_range:
            if current_range["server_uuid"] == server_uuid and current_range["end"] + 1 == gno:
                current_range["end"] = gno
                current_range["end_ts"] = timestamp
            else:
                yield current_range
                current_range = None

        if not current_range:
            current_range = {
                "end": gno,
                "end_ts": timestamp,
                "server_id": server_id,
                "server_uuid": server_uuid,
                "start": gno,
                "start_ts": timestamp,
            }

    if current_range:
        yield current_range


def partition_sort_and_combine_gtid_ranges(ranges):
    """Partitions GTID ranges by UUID, sort them and then combine any duplicates / continuous ranges like
    [(1, 3), (2, 3), (5, 7), (6, 8), (8, 9)] => [(1, 3), (5, 9)]"""
    tuples = collections.defaultdict(list)
    for rng in ranges:
        tuples[rng["server_uuid"]].append([rng["start"], rng["end"]])
    combined = {}
    for server_uuid, server_ranges in tuples.items():
        server_ranges = sorted(server_ranges)
        combined_ranges = []
        last_start = None
        last_end = None
        for rng in server_ranges:
            start, end = rng
            if last_start is None:
                last_start = start
            if last_end is not None:
                # New range is fully contained within last range, skip
                if start <= last_end and end <= last_end:
                    continue
                # Extends current end or continuation of previous range
                if start <= last_end or last_end + 1 == start:
                    last_end = end
                    continue
                combined_ranges.append([last_start, last_end])
            last_start = start
            last_end = end
        if last_start is not None and last_end is not None:
            combined_ranges.append([last_start, last_end])
        combined[server_uuid] = combined_ranges

    return combined


def first_contains_gtids_not_in_second(first, second):
    """Returns True if list of GTIDs identified by `first` contains any entry that is not present in `second`.
    Because our binlog specific ranges only contain the actual GTIDs in a particular file this method assumes
    that all GNOs below current GNO have been included. So if second has uuid1:20 and first has uuid1:19 the
    second is assumed to contain everything that is in the first one."""
    first_uuid_gnos = partition_sort_and_combine_gtid_ranges(first)
    second_uuid_gnos = partition_sort_and_combine_gtid_ranges(second)
    if set(first_uuid_gnos) - set(second_uuid_gnos):
        return True
    for server_uuid, first_ranges in first_uuid_gnos.items():
        second_ranges = second_uuid_gnos[server_uuid]
        # We don't expect any holes in the sequences so these loops should practically consist of single iteration
        for _, first_end in first_ranges:
            if not second_ranges[-1][-1] >= first_end:
                return True
    return False


def make_gtid_range_string(ranges):
    """Produces minimal string representation of given list of GTID ranges. The string is
    compatible with MySQL's GTID functions (server_uuid1:gno1-gno2:gno3,server_uuid2:gno4)"""
    uuid_gnos = []
    for server_uuid, server_ranges in partition_sort_and_combine_gtid_ranges(ranges).items():
        range_strs = []
        for rng in server_ranges:
            if rng[0] == rng[1]:
                range_strs.append(str(rng[0]))
            else:
                range_strs.append("{}-{}".format(rng[0], rng[1]))
        uuid_gnos.append("{}:{}".format(server_uuid, ":".join(range_strs)))
    return ",".join(uuid_gnos)


def parse_gtid_range_string(input_range):
    """Converts a string like "uuid1:id1-id2:id3:id4-id5, uuid2:id6" into a dict like
    {"uuid1": [[id1, id2], [id3, id3], [id4, id5]], "uuid2": [[id6, id6]]}. ID ranges are
    sorted from lowest to highest"""
    # This might be called again for a value that has already been processed
    if isinstance(input_range, dict):
        return input_range
    elif not input_range or not input_range.strip():
        return {}

    by_uuid = collections.defaultdict(list)
    for uuid_and_ranges in input_range.split(","):
        server_uuid, *ranges = uuid_and_ranges.strip().split(":")
        all_ranges = by_uuid[server_uuid.lower()]
        for rng in ranges:
            if "-" in rng:
                low, high = rng.split("-")
            else:
                low, high = rng, rng
            all_ranges.append([int(low), int(high)])
    return {server_uuid: sorted(ranges, key=lambda rng: rng[0]) for server_uuid, ranges in by_uuid.items()}


def add_gtid_ranges_to_executed_set(existing_set, *new_ranges):
    """Takes in a dict like {"uuid1": [[1, 4], [7, 12]], "uuid2": [[1, 100]]} (as returned by e.g. parse_gtid_range_string)
    and any number of lists of type [{"server_uuid": "uuid", "start": 1, "end": 3}, ...]. Adds all the ranges in the lists to
    the ranges in the dict and returns a new dict that contains minimal representation with both the old and new ranges."""
    all_ranges = []
    for server_uuid, ranges in existing_set.items():
        for rng in ranges:
            all_ranges.append({
                "end": rng[1],
                "server_uuid": server_uuid,
                "start": rng[0],
            })
    for rng in new_ranges:
        all_ranges.extend(rng)
    return partition_sort_and_combine_gtid_ranges(all_ranges)


def truncate_gtid_executed(gtid_executed, truncate_to):
    """Truncates (in place) the gtid_executed dict so that gtid_executed does not include any transactions that are not
    included in the truncate_to string (which must be in the format "server_uuid1:gno1,server_uuid2:gno2"). The
    truncation is only performed for servers included in the truncate_to string, for other servers the executed
    transactions are left intact."""
    server_gnos = parse_gtid_range_string(truncate_to)
    for server_uuid, gnos in server_gnos.items():
        if not gnos:
            continue
        gno = max(max(low_high) for low_high in gnos)
        server_executed = gtid_executed[server_uuid]
        truncated_executed = []
        for start, end in server_executed:
            if end < gno:
                truncated_executed.append([start, end])
            elif start <= gno:
                truncated_executed.append([start, gno])
                break
            else:
                break
        gtid_executed[server_uuid] = truncated_executed


def are_gtids_in_executed_set(gtid_executed, ranges, *, exclude_uuid=None):
    """Takes a dict of {uuid: [[start1, end1], ...]} mappings and binlog ranges as arguments and determines whether
    the dict contains all of the binlog ranges."""
    if not gtid_executed:
        return False
    for rng in ranges:
        if rng["server_uuid"] == exclude_uuid:
            continue
        if rng["server_uuid"] not in gtid_executed:
            return False
        server_executed = gtid_executed[rng["server_uuid"]]
        # The binlog ranges may contain only individual GTIDs instead of full 1-N ranges. We assume the backed up
        # GTID sequences are uninterrupted and backed up in correct order so if any server range has an end that is
        # at least as large as the range we're checking against consider the data to be backed up.
        if not any(executed_rng[1] >= rng["end"] for executed_rng in server_executed):
            return False
    return True


@contextlib.contextmanager
def mysql_connection(*, ca_file=None, db="mysql", host="127.0.0.1", password, port, timeout=DEFAULT_MYSQL_TIMEOUT, user):
    ssl = None
    if ca_file:
        ssl = {"ca": ca_file}
    connection = pymysql.connect(
        charset="utf8mb4",
        connect_timeout=timeout,
        cursorclass=pymysql.cursors.DictCursor,
        db=db,
        host=host,
        password=password,
        read_timeout=timeout,
        port=port,
        ssl=ssl,
        user=user,
        write_timeout=timeout,
    )
    try:
        yield connection
    finally:
        connection.close()


@contextlib.contextmanager
def mysql_cursor(*, ca_file=None, db="mysql", host="127.0.0.1", password, port, timeout=DEFAULT_MYSQL_TIMEOUT, user):
    with mysql_connection(
        ca_file=ca_file, db=db, host=host, password=password, port=port, timeout=timeout, user=user
    ) as connection:
        with connection.cursor() as cursor:
            yield cursor


def rsa_encrypt_bytes(rsa_public_key_pem, data):
    rsa_public_key = serialization.load_pem_public_key(rsa_public_key_pem, backend=default_backend())
    pad = padding.OAEP(mgf=padding.MGF1(algorithm=SHA1()), algorithm=SHA1(), label=None)
    return rsa_public_key.encrypt(data, pad)


def rsa_decrypt_bytes(rsa_private_key_pem, data):
    rsa_private_key = serialization.load_pem_private_key(data=rsa_private_key_pem, password=None, backend=default_backend())
    pad = padding.OAEP(mgf=padding.MGF1(algorithm=SHA1()), algorithm=SHA1(), label=None)
    return rsa_private_key.decrypt(data, pad)


def sort_and_filter_binlogs(*, binlogs, last_index, log, promotions):
    """Given a list of remote binlogs and list of promotions, sorts the binlogs and returns the ones that
    are from servers that have been valid masters at the time of generating the binlog entry; any binlogs
    that are from servers that have been replaced before the binlog was uploaded are ignored."""
    ranges = []
    last_range_start = None
    for range_start in sorted(promotions):
        if last_range_start is not None:
            ranges.append([last_range_start, range_start - 1, promotions[last_range_start]])
        last_range_start = range_start
    ranges.append([last_range_start, 2 ** 31, promotions[last_range_start]])

    binlogs.sort(key=lambda bl: (bl["remote_index"], bl["server_id"]))
    valid_binlogs = []

    for binlog in binlogs:
        index = binlog["remote_index"]
        server_id = None  # Keep static checkers happy, `ranges` always has matching element
        # Unless there's some pathological server replacement cycle the list is short and inner loop is fine
        for range_start, range_end, server_id in ranges:
            if range_start <= index <= range_end:
                break
        if binlog["server_id"] != server_id:
            # This can happen if old master was still alive when new one was promoted. This is
            # something that is expected to happen sometimes and is handled gracefully here.
            # Log a warning still to make this more visible in logs in case something does go wrong.
            log.warning(
                "Binlog %s from server %s ignored because server %s is valid for that index: %r", index, binlog["server_id"],
                server_id, ranges
            )
        else:
            adjusted_index = binlog.get("adjusted_remote_index", index)
            if adjusted_index != last_index + 1:
                raise Exception(
                    f"Binlog sequence has a gap, expected {last_index + 1} after {last_index}, found {adjusted_index}: "
                    f"{binlog} / {binlogs}"
                )
            valid_binlogs.append(binlog)
            last_index = adjusted_index

    return valid_binlogs


def detect_running_process_id(command):
    """Find a process with matching command owned by the same user as current process and return
    its pid. Returns None if no such process is found."""
    # This is mainly used in tests. Actual use should rely on systemd and if non-Linux operating systems
    # are supported later whatever service management interface those other operating systems provide
    output = subprocess.check_output(["ps", "-x", "--cols", "1000", "-o", "pid,command"])
    # We don't expect to have non-ASCII characters, convert using ISO-8859-1, which should always work without raising
    output = output.decode("ISO-8859-1")
    regex = re.compile(r"^\s*\d+\s+{}".format(re.escape(command)))
    ids = [int(line.strip().split()[0]) for line in output.splitlines() if regex.match(line)]
    if not ids or len(ids) > 1:
        return None
    return ids[0]


def wait_for_port(*, host, port, timeout):
    """Waits until given (address, port) tuple starts listening or given timeout is exceeded"""
    start_time = time.monotonic()
    while time.monotonic() - start_time < timeout:
        with contextlib.suppress(Exception):
            sock = socket.create_connection(address=(host, port), timeout=1)
            sock.close()
            return
        time.sleep(0.1)

    raise Exception(f"Could not connect to {host}:{port} in {timeout} seconds")


def relay_log_name(*, prefix, index, full_path=True):
    name = f"{prefix}.{index:06}"
    if not full_path:
        name = os.path.basename(name)
    return name


def track_rate(*, current, last_recorded, last_recorded_time, metric_name, min_increase=1_000_000, stats):
    """Calculates rate of change given current value and previously handled value and time. If there is
    a relevant change (as defined by min_increase) and some time has passed, the current rate of change
    is sent as integer gauge using given stats client and metric name. Returns the values to pass to the
    next invocation of this function."""
    now = time.monotonic()
    return_value, return_time = last_recorded, last_recorded_time
    if current > last_recorded + min_increase and now > last_recorded_time:
        time_elapsed = now - last_recorded_time
        diff = current - last_recorded
        per_second = int(diff / time_elapsed)
        if per_second > 0:
            stats.gauge_int(metric_name, per_second)
            return_value = current
            return_time = now
    return return_value, return_time
