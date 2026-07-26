import plistlib
from pathlib import Path

ROOT = Path(__file__).parents[1]
LAUNCHD = ROOT / "launchd"
SCRIPTS = ROOT / "scripts"


def rendered_plist(name: str) -> dict:
    payload = (LAUNCHD / name).read_text()
    replacements = {
        "__PROJECT_ROOT__": str(ROOT),
        "__PYTHON__": str(ROOT / ".venv/bin/python"),
        "__LOG_DIR__": "/Users/test/Library/Logs/Stock EVA",
        "__BACKUP_ROOT__": "/Users/test/Library/Application Support/Stock EVA/backups",
    }
    for token, value in replacements.items():
        payload = payload.replace(token, value)
    assert "__" not in payload
    return plistlib.loads(payload.encode())


def test_api_and_workspace_agents_are_local_only_and_recoverable() -> None:
    api = rendered_plist("com.finlay.stock-eva.api.plist.in")
    web = rendered_plist("com.finlay.stock-eva.web.plist.in")

    assert api["RunAtLoad"] is True
    assert api["KeepAlive"] is True
    assert api["WorkingDirectory"] == str(ROOT)
    assert api["ProgramArguments"][-4:] == [
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
    ]
    assert web["RunAtLoad"] is True
    assert web["KeepAlive"] is True
    assert web["ProgramArguments"][-4:] == [
        "--bind",
        "127.0.0.1",
        "--directory",
        str(ROOT),
    ]


def test_after_close_agent_uses_idempotent_backend_schedule() -> None:
    refresh = rendered_plist("com.finlay.stock-eva.refresh.plist.in")

    assert refresh["ProgramArguments"][-4:] == [
        "-m",
        "backend.app.cli",
        "auto-refresh-once",
        "--execute",
    ]
    assert refresh["WorkingDirectory"] == str(ROOT)
    assert refresh["StartCalendarInterval"] == [
        {"Hour": 18, "Minute": 10},
        {"Hour": 18, "Minute": 40},
        {"Hour": 19, "Minute": 20},
        {"Hour": 20, "Minute": 10},
        {"Hour": 21, "Minute": 0},
        {"Hour": 7, "Minute": 15},
    ]
    assert "KeepAlive" not in refresh


def test_calendar_and_private_backup_agents_have_bounded_scopes() -> None:
    calendar = rendered_plist("com.finlay.stock-eva.calendar.plist.in")
    backup = rendered_plist("com.finlay.stock-eva.backup.plist.in")

    assert calendar["RunAtLoad"] is True
    assert calendar["StartCalendarInterval"] == [
        {"Hour": 16, "Minute": 30},
        {"Day": 1, "Hour": 4, "Minute": 5},
    ]
    assert calendar["ProgramArguments"][-5:] == [
        "-m",
        "backend.app.cli",
        "calendar-sync",
        "--startup",
        "--execute",
    ]
    assert backup["StartCalendarInterval"] == {"Hour": 2, "Minute": 30}
    assert backup["ProgramArguments"][-8:] == [
        "--source",
        str(ROOT / "var/user/stock_eva_user.sqlite3"),
        "--backup-root",
        "/Users/test/Library/Application Support/Stock EVA/backups",
        "--keep-daily",
        "7",
        "--keep-weekly",
        "4",
    ]


def test_launchagent_assets_never_embed_credentials_or_remote_mount_actions() -> None:
    text = "\n".join(path.read_text() for path in sorted(LAUNCHD.glob("*.plist.in"))).lower()

    for forbidden in (
        "password",
        "passwd",
        "token",
        "apikey",
        "api_key",
        "smb://",
        "mount_smbfs",
    ):
        assert forbidden not in text


def test_management_scripts_require_explicit_mutation_flags() -> None:
    installer = (SCRIPTS / "stock_eva_launchagents_install.sh").read_text()
    uninstaller = (SCRIPTS / "stock_eva_launchagents_uninstall.sh").read_text()
    status = (SCRIPTS / "stock_eva_launchagents_status.sh").read_text()

    assert "--install" in installer
    assert "MODE=check" in installer
    assert "--uninstall" in uninstaller
    assert "MODE=check" in uninstaller
    assert "launchctl print" in status
    assert "launchctl bootstrap" in installer
    assert "launchctl bootout" in uninstaller
