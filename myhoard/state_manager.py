# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import errno
import json
import os
import threading

from .util import atomic_create_file


class StateManager:
    """Simple disk backed dict/JSON state manager"""

    def __init__(self, *, allow_unknown_keys=False, lock=None, state, state_file):
        self.allow_unknown_keys = allow_unknown_keys
        self.lock = lock or threading.RLock()
        self.state = state

        # Check that the state_file directory actually exists before initializing. If it doesn't
        # then we'll just crash out later when we try to write to it. We'd prefer to crash here,
        # where we're more likely to find the problem higher up the stack.
        state_file_dirname = os.path.dirname(state_file)
        if not os.path.isdir(state_file_dirname):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), state_file_dirname)

        self.state_file = state_file
        self.read_state()

    def delete_state(self):
        if os.path.exists(self.state_file):
            os.remove(self.state_file)

    def increment_counter(self, *, name, increment=1):
        with self.lock:
            assert name in self.state
            self.state[name] = self.state[name] + increment
            self.write_state()

    def read_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                self.state.clear()
                self.state.update(**json.load(f))
        else:
            self.write_state()

    def update_state(self, **kwargs):
        with self.lock:
            changes = False
            for name, value in kwargs.items():
                if not self.allow_unknown_keys:
                    assert name in self.state
                if self.state.get(name) != value:
                    self.state[name] = value
                    changes = True
            if changes:
                self.write_state()

    def write_state(self):
        with atomic_create_file(self.state_file) as f:
            json.dump(self.state, f)
