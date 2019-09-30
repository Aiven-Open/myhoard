# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import io
import json
import logging
import os
import threading

from .util import atomic_create_file


class AppendOnlyStateManager:
    """Disk backed state manager that stores a list of dict/JSON objects. Entries can only be added to the
    end of the list and removed from the beginning. The entire file is rewritten when sufficient number
    of entries have been removed from the beginning. Otherwise deletions are handled by writing invalidation
    entry for current head entry to the end of the file.

    Uses textual header instead of binary as well as linefeed after each row to make the file human readable."""

    HEADER_BYTES = 9
    ENTRY_TYPE_TOMBSTONE = "T"
    ENTRY_TYPE_JSON = "J"

    MAX_DEAD_ENTRY_COUNT = 1000

    def __init__(self, *, entries, lock=None, state_file):
        self.dead_entry_count = 0
        self.entries = entries
        self.lock = lock or threading.RLock()
        self.log = logging.getLogger(self.__class__.__name__)
        self.max_dead_entry_count = self.MAX_DEAD_ENTRY_COUNT
        self.state_file = state_file
        self.read_state()

    def append(self, entry):
        self.append_many([entry])

    def append_many(self, entries):
        if not entries:
            return

        with self.lock:
            full_data = self._encode_entries(entries)
            with open(self.state_file, "ab") as f:
                f.write(full_data)
            self.entries.extend(entries)

    def read_state(self):
        entries = []
        pos = 0
        if os.path.exists(self.state_file):
            with open(self.state_file, "rb") as f:
                if not isinstance(f, io.BufferedReader):
                    f = io.BufferedReader(f)
                while True:
                    header = f.read(self.HEADER_BYTES).decode("utf-8")
                    if not header:
                        break
                    if len(header) < self.HEADER_BYTES:
                        raise EOFError(
                            f"Unexpected end of file at {pos}. Expected {self.HEADER_BYTES} bytes, got only {len(header)}"
                        )

                    entry_type = header[0]
                    length = int(header[1:], 16)
                    data = f.read(length)
                    if len(data) < length:
                        raise EOFError(f"Expected {length} bytes of data for entry at {pos}, only got {len(data)} bytes")

                    if entry_type == self.ENTRY_TYPE_TOMBSTONE:
                        self.dead_entry_count += 1
                    elif entry_type == self.ENTRY_TYPE_JSON:
                        entries.append(data)
                    else:
                        raise ValueError(f"Unsupported entry type {entry_type} at position {pos}")
                    pos += len(header) + len(data)
        else:
            self._rewrite_file(entries=entries)

        self.log.info(
            "Loaded %s entries from %r, %s of them are marked as dead", len(entries), self.state_file, self.dead_entry_count
        )
        self.entries.extend(json.loads(entry.decode("utf-8")) for entry in entries[self.dead_entry_count:])

    def remove_head(self):
        self.remove_many_from_head(count=1)

    def remove_many_from_head(self, count):
        if count <= 0:
            return
        elif count > len(self.entries):
            raise ValueError(f"Requested removal of {count} entries when only {len(self.entries)} exist")

        with self.lock:
            new_entries = self.entries[count:]
            if self.dead_entry_count + count > self.max_dead_entry_count:
                self.log.info(
                    "Dead entry count %s exceeds %s for %r, rewriting file", self.dead_entry_count + count,
                    self.max_dead_entry_count, self.state_file
                )
                self._rewrite_file(entries=new_entries)
                self.dead_entry_count = 0
            else:
                # All entries look identical: just linefeed as data (to keep the file human readable) and header
                # says its a tombstone entry with one byte of data
                line = self._make_header(entry_type=self.ENTRY_TYPE_TOMBSTONE, data=b"\n") + b"\n"
                # Construct full data with join for better performance in case count is large
                data = b"".join(line for _ in range(count))
                with open(self.state_file, "ab") as f:
                    f.write(data)
                self.dead_entry_count += count
            self.entries[:] = new_entries

    @classmethod
    def _encode_entries(cls, entries):
        encoded = [json.dumps(entry, ensure_ascii=True).encode("utf-8").rstrip(b"\n") + b"\n" for entry in entries]
        full_data = []
        for entry_data in encoded:
            full_data.append(cls._make_header(entry_type=cls.ENTRY_TYPE_JSON, data=entry_data))
            full_data.append(entry_data)
        return b"".join(full_data)

    @staticmethod
    def _make_header(*, entry_type, data):
        return f"{entry_type}{len(data):08x}".encode("utf-8")

    def _rewrite_file(self, *, entries):
        full_data = self._encode_entries(entries)
        with atomic_create_file(self.state_file, binary=True) as f:
            f.write(full_data)
