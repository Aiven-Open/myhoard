# Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
from types import TracebackType
from typing import BinaryIO, Iterable, Iterator, Type


class MethodNotSupportedError(Exception):
    pass


class BinaryIOSlice(BinaryIO):
    def __init__(self, max_file_size: int, stream: BinaryIO):
        super().__init__()
        self._max_file_size = max_file_size
        self._size_remaining = max_file_size
        self.stream = stream

    def read(self, __n: int = -1) -> bytes:
        if __n < 0:
            to_read = self._size_remaining
        else:
            to_read = min(__n, self._size_remaining)
        result = self.stream.read(to_read)

        if result:
            self._size_remaining -= len(result)

        return result

    @property
    def mode(self) -> str:
        return self.stream.mode

    @property
    def name(self) -> str:
        return self.stream.name

    def close(self) -> None:
        self.stream.close()

    @property
    def closed(self) -> bool:
        return self.stream.closed

    def fileno(self) -> int:
        return self.stream.fileno()

    def flush(self) -> None:
        return self.stream.flush()

    def isatty(self) -> bool:
        return self.stream.isatty()

    def readable(self) -> bool:
        return self.stream.readable()

    def readline(self, __limit: int = -1) -> bytes:
        raise MethodNotSupportedError()

    def readlines(self, __hint: int = -1) -> list[bytes]:
        raise MethodNotSupportedError()

    def seek(self, __offset: int, __whence: int = 0) -> int:
        return self.stream.seek(__offset, __whence)

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._max_file_size - self._size_remaining

    def truncate(self, __size: int | None = None) -> int:
        raise MethodNotSupportedError()

    def writable(self) -> bool:
        return False

    def write(self, __s: bytes) -> int:  # type: ignore[override]
        raise MethodNotSupportedError()

    def writelines(self, __lines: Iterable[bytes]) -> None:  # type: ignore[override]
        raise MethodNotSupportedError()

    def __next__(self) -> bytes:
        return self.stream.__next__()

    def __iter__(self) -> Iterator[bytes]:
        return self.stream.__iter__()

    def __enter__(self) -> BinaryIO:
        return self.stream.__enter__()

    def __exit__(
        self, __t: Type[BaseException] | None, __value: BaseException | None, __traceback: TracebackType | None
    ) -> None:
        return self.stream.__exit__(__t, __value, __traceback)
