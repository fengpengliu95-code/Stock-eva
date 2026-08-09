from datetime import date

from backend.app.cli import build_parser


def test_snapshot_cli_defaults_to_read_only_plan() -> None:
    args = build_parser().parse_args(
        [
            "market-regime-snapshots",
            "--start",
            "2026-07-01",
            "--end",
            "2026-07-31",
        ]
    )

    assert args.command == "market-regime-snapshots"
    assert args.start == date(2026, 7, 1)
    assert args.end == date(2026, 7, 31)
    assert args.execute is False
