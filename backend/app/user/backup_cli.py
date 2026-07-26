"""Standalone local-private-data backup command.

This entry point intentionally does not depend on market storage or networking:
`python -m backend.app.user.backup_cli --source ... --backup-root ...`.
"""

import argparse
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from backend.app.user.backup import PrivateBackupError, PrivateBackupService


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a consistent local Stock EVA private SQLite backup",
    )
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--backup-root", required=True, type=Path)
    parser.add_argument("--keep-daily", type=int, default=7)
    parser.add_argument("--keep-weekly", type=int, default=4)
    parser.add_argument(
        "--at",
        type=datetime.fromisoformat,
        help="override the timestamp for deterministic maintenance/testing",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    now = args.at or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    try:
        outcome = PrivateBackupService(
            args.source,
            args.backup_root,
            keep_daily=args.keep_daily,
            keep_weekly=args.keep_weekly,
        ).run(now)
    except (PrivateBackupError, ValueError):
        print(
            json.dumps(
                {
                    "status": "error",
                    "error_code": "private_backup_failed",
                },
                ensure_ascii=False,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "status": outcome.status,
                "integrity_check": outcome.integrity_check,
                "completed_at": outcome.completed_at.isoformat(),
                "daily_snapshot": outcome.daily_backup.name,
                "weekly_snapshot": outcome.weekly_backup.name,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
