# Copyright (c) 2023 Aiven, Helsinki, Finland. https://aiven.io/
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import logging
import os
import time


def get_logger_name() -> str:
    """Get the name of the logger for the current test.

    Environment variable PYTEST_CURRENT_TEST is set by pytest and contains something like
    ``"test/test_myfile.py::test_something (call)"``.

    With the example above, this function will return ``"test.test_myfile.test_something"``.

    If the environment variable is not set, it will return something like ``"test.test_myfile"``.
    """
    current_test = os.environ.get("PYTEST_CURRENT_TEST")
    if current_test is None:
        return f"test.{Path(__file__).stem}"

    path, _, name = current_test.split(":")
    name = name.split()[0]
    path = path.removesuffix(".py").replace("/", ".").replace("\\", ".")
    return f"{path}.{name}"


def log_duration(function: Callable) -> Callable:
    """Decorator to log the duration of a function call."""
    description = function.__name__.replace("_", " ").capitalize()

    @wraps(function)
    def wrapper(*args, **kwargs) -> Any:
        logger = logging.getLogger(get_logger_name())

        t0 = time.monotonic_ns()
        result = function(*args, **kwargs)
        t1 = time.monotonic_ns()

        logger.info("%s took %.5f sec.", description, (t1 - t0) / 1_000_000_000)

        return result

    return wrapper
