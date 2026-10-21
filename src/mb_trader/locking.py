from __future__ import annotations

import os
from pathlib import Path
from typing import IO

from .errors import ConfigurationError


class ProcessLock:
    """Non-blocking, cross-platform one-writer lock held for a CLI session."""

    def __init__(self, path: Path):
        self.path = path
        self.handle: IO[bytes] | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            self.handle = None
            raise ConfigurationError(
                f"another process owns broker session/database lock {self.path}"
            ) from exc

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None

    def __enter__(self) -> ProcessLock:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()
