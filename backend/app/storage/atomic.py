"""Native, descriptor-relative no-clobber publication for Linux and macOS."""

import ctypes
import errno
import os
import sys
from pathlib import Path


def rename_noreplace_bound(
    source_parent: int, source: str, destination_parent: int, destination: str
) -> None:
    """Publish one leaf atomically; never fall back to check-then-rename."""
    for name in (source, destination):
        if not name or name in {".", ".."} or "/" in name or "\0" in name:
            raise ValueError("atomic publication requires leaf names")
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        function = getattr(library, "renameatx_np", None)
        flags = 0x00000004  # RENAME_EXCL
    elif sys.platform.startswith("linux"):
        function = getattr(library, "renameat2", None)
        flags = 1  # RENAME_NOREPLACE
    else:
        function = None
        flags = 0
    if function is None:
        raise OSError(errno.ENOTSUP, "atomic no-clobber publication is unavailable")
    function.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    function.restype = ctypes.c_int
    ctypes.set_errno(0)
    if (
        function(
            source_parent, os.fsencode(source), destination_parent, os.fsencode(destination), flags
        )
        != 0
    ):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), destination)


def rename_noreplace(source: Path, destination: Path) -> None:
    """Open both parents without following symlinks and use the native operation."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    source_parent = os.open(source.parent, flags)
    try:
        destination_parent = os.open(destination.parent, flags)
        try:
            rename_noreplace_bound(source_parent, source.name, destination_parent, destination.name)
        finally:
            os.close(destination_parent)
    finally:
        os.close(source_parent)
