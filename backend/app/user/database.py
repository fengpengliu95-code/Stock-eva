from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Lock

_locks_guard = Lock()
_initialization_locks: dict[Path, Lock] = {}


@contextmanager
def user_database_initialization(path: Path) -> Iterator[None]:
    resolved_path = path.resolve()
    with _locks_guard:
        initialization_lock = _initialization_locks.setdefault(
            resolved_path,
            Lock(),
        )
    with initialization_lock:
        yield
