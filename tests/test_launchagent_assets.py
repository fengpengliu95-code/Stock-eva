import plistlib
import shutil
import stat
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from backend.app.config import Settings
from backend.app.market.calendar_sync import CalendarSyncPolicy
from backend.app.user.backup import PrivateBackupError, PrivateBackupService

ROOT = Path(__file__).parents[1]
LAUNCHD = ROOT / "launchd"
SCRIPTS = ROOT / "scripts"
RUNTIME_ROOT = Path("/Users/test/Library/Application Support/Stock EVA/runtime")
RUNTIME_CURRENT = RUNTIME_ROOT / "current"
CONFIG_ROOT = Path("/Users/test/Library/Application Support/Stock EVA/config")
DATA_ROOT = Path("/Users/test/Library/Application Support/Stock EVA/data")


def rendered_plist(name: str) -> dict:
    payload = (LAUNCHD / name).read_text()
    replacements = {
        "__CONFIG_ROOT__": str(CONFIG_ROOT),
        "__PUBLIC_ROOT__": str(RUNTIME_CURRENT / "public"),
        "__PYTHON__": str(RUNTIME_CURRENT / ".venv/bin/python"),
        "__DATA_ROOT__": str(DATA_ROOT),
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
    assert api["WorkingDirectory"] == str(CONFIG_ROOT)
    assert api["ProgramArguments"][-4:] == [
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
    ]
    assert web["RunAtLoad"] is True
    assert web["KeepAlive"] is True
    assert web["WorkingDirectory"] == str(CONFIG_ROOT)
    assert web["ProgramArguments"] == [
        str(RUNTIME_CURRENT / ".venv/bin/python"),
        "-m",
        "backend.app.static_server",
        "--host",
        "127.0.0.1",
        "--port",
        "8080",
        "--directory",
        str(RUNTIME_CURRENT / "public"),
    ]


def test_after_close_agent_uses_idempotent_backend_schedule() -> None:
    refresh = rendered_plist("com.finlay.stock-eva.refresh.plist.in")

    assert refresh["ProgramArguments"][-4:] == [
        "-m",
        "backend.app.cli",
        "auto-refresh-once",
        "--execute",
    ]
    assert refresh["WorkingDirectory"] == str(CONFIG_ROOT)
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
    assert backup.get("RunAtLoad") is None
    assert backup.get("KeepAlive") is None
    assert backup["ProgramArguments"][-9:] == [
        "-m",
        "backend.app.cli",
        "backup-private-data",
        "--backup-root",
        "/Users/test/Library/Application Support/Stock EVA/backups",
        "--keep-daily",
        "7",
        "--keep-weekly",
        "4",
    ]
    assert Settings.model_config["env_file"] == ".env"


def test_calendar_1630_schedule_wins_over_run_at_load_startup_flag() -> None:
    decision = CalendarSyncPolicy().decide(
        datetime(2026, 7, 27, 16, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        state=None,
        startup=True,
    )

    assert decision.action == "light"
    assert decision.reason == "daily_1630_check"


def test_settings_reads_dotenv_from_launchagent_working_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / ".env").write_text(
        "STOCK_EVA_AUTO_REFRESH_ENABLED=true\n"
        "STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/synthetic\n"
    )
    monkeypatch.chdir(tmp_path)

    settings = Settings()

    assert settings.auto_refresh_enabled is True
    assert settings.nas_market_dataset_root == Path("/Volumes/Stock/synthetic")


def test_missing_private_database_fails_once_without_creating_backup(
    tmp_path: Path,
) -> None:
    backup_root = tmp_path / "backups"

    with pytest.raises(PrivateBackupError, match="source is unavailable"):
        PrivateBackupService(
            tmp_path / "missing.sqlite3",
            backup_root,
        ).run(datetime.now(ZoneInfo("Asia/Shanghai")))

    assert not backup_root.exists()


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
    assert '"$LAUNCHCTL" bootstrap' in installer
    assert '"$LAUNCHCTL" bootout' in uninstaller
    assert '/bin/mv -fh "$NEXT_CURRENT" "$RUNTIME_CURRENT"' in installer
    assert "wait_for_http" in status
    assert '[[ "$attempt" -le 15 ]]' in status


def synthetic_project(tmp_path: Path, *, user_dir: str = "var/user") -> Path:
    project = tmp_path / "Stock EVA Synthetic"
    shutil.copytree(LAUNCHD, project / "launchd")
    python = project / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/bin/sh\nexit 0\n")
    python.chmod(0o755)
    for directory in ("backend", "workspace", "dashboard", "docs"):
        target = project / directory
        target.mkdir()
        (target / "sentinel.txt").write_text(directory)
    (project / "workspace/package.json").write_text(
        '{"scripts":{"build":"vite build"},"dependencies":{"klinecharts":"10.0.0"}}'
    )
    (project / "workspace/package-lock.json").write_text('{"name":"synthetic","lockfileVersion":3}')
    (project / "index.html").write_text("stock eva")
    (project / "pyproject.toml").write_text("[project]\nname='stock-eva-synthetic'\n")
    (project / "uv.lock").write_text("synthetic lock")
    (project / "README.md").write_text("synthetic")
    for directory in ("market", "control", "user"):
        target = project / "var" / directory
        target.mkdir(parents=True)
        (target / f"{directory}.sentinel").write_text(directory)
    (project / ".env").write_text(
        "\n".join(
            [
                "STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/stock-eva-market",
                "STOCK_EVA_AUTO_REFRESH_ENABLED=false",
                f"STOCK_EVA_USER_DATA_DIR={user_dir}",
                "STOCK_EVA_USER_DATABASE_NAME=stock_eva_user.sqlite3",
            ]
        )
        + "\n"
    )
    return project


def fake_launchctl(tmp_path: Path) -> Path:
    executable = tmp_path / "fake-launchctl"
    executable.write_text(
        """#!/bin/bash
echo "$*" >> "$CALL_LOG"
if [[ "$1" == "print" ]]; then
  [[ "${LOADED_LABEL:-}" != "" && "$2" == *"/$LOADED_LABEL" ]]
  exit
fi
if [[ "$1" == "bootstrap" && "${FAIL_LABEL:-}" != "" && "$*" == *"$FAIL_LABEL"* ]]; then
  exit 9
fi
exit 0
"""
    )
    executable.chmod(0o755)
    return executable


def fake_lsof(tmp_path: Path, *, occupied: bool) -> Path:
    executable = tmp_path / "fake-lsof"
    executable.write_text(f"#!/bin/sh\nexit {0 if occupied else 1}\n")
    executable.chmod(0o755)
    return executable


def stateful_fake_launchctl(tmp_path: Path) -> Path:
    executable = tmp_path / "stateful-fake-launchctl"
    executable.write_text(
        """#!/bin/bash
state="$LAUNCHCTL_STATE"
target="$(/usr/bin/readlink "$RUNTIME_CURRENT_PATH" 2>/dev/null || true)"
echo "$*" >> "$CALL_LOG"
case "$1" in
  print)
    label="${2##*/}"
    echo "launchctl|print|$label|$target" >> "$HANDOFF_EVENT_LOG"
    /usr/bin/grep -Fxq "$label" "$state"
    ;;
  bootout)
    label="${2##*/}"
    echo "launchctl|bootout|$label|$target" >> "$HANDOFF_EVENT_LOG"
    if [[ "${FAIL_BOOTOUT_LABEL:-}" == "$label" ]]; then
      exit 8
    fi
    /usr/bin/grep -Fxv "$label" "$state" > "$state.next" || true
    /bin/mv -f "$state.next" "$state"
    ;;
  bootstrap)
    label="${3##*/}"
    label="${label%.plist}"
    echo "launchctl|bootstrap|$label|$target" >> "$HANDOFF_EVENT_LOG"
    if [[ "${FAIL_LABEL:-}" == "$label" ]]; then
      exit 9
    fi
    fail_once_marker="$state.fail-once.$label"
    if [[ "${FAIL_ONCE_LABEL:-}" == "$label" && ! -f "$fail_once_marker" ]]; then
      /usr/bin/touch "$fail_once_marker"
      exit 9
    fi
    if [[ "${BOOTSTRAP_NO_LOAD_LABEL:-}" == "$label" ]]; then
      exit 0
    fi
    if ! /usr/bin/grep -Fxq "$label" "$state"; then
      echo "$label" >> "$state"
    fi
    ;;
esac
"""
    )
    executable.chmod(0o755)
    return executable


def stateful_fake_lsof(tmp_path: Path) -> Path:
    executable = tmp_path / "stateful-fake-lsof"
    executable.write_text(
        """#!/bin/bash
port=""
for argument in "$@"; do
  case "$argument" in
    -iTCP:*) port="${argument#-iTCP:}" ;;
  esac
done
case "$port" in
  8000) label="com.finlay.stock-eva.api" ;;
  8080) label="com.finlay.stock-eva.web" ;;
  *) label="" ;;
esac
occupied=0
if [[ "$port" == "8080" && "${STICKY_WEB_PORT:-0}" == "1" ]]; then
  occupied=1
elif [[ -n "$label" ]] && /usr/bin/grep -Fxq "$label" "$LAUNCHCTL_STATE"; then
  occupied=1
fi
target="$(/usr/bin/readlink "$RUNTIME_CURRENT_PATH" 2>/dev/null || true)"
echo "lsof|$port|$target|$occupied" >> "$HANDOFF_EVENT_LOG"
if [[ "$occupied" == "1" ]]; then
  exit 0
fi
exit 1
"""
    )
    executable.chmod(0o755)
    return executable


def fake_uv(tmp_path: Path) -> Path:
    executable = tmp_path / "fake-uv"
    executable.write_text(
        """#!/bin/bash
set -e
mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"
cat >"$UV_PROJECT_ENVIRONMENT/bin/python" <<'PY'
#!/bin/sh
exit 0
PY
chmod 755 "$UV_PROJECT_ENVIRONMENT/bin/python"
"""
    )
    executable.chmod(0o755)
    return executable


def fake_npm(tmp_path: Path) -> Path:
    executable = tmp_path / "fake-npm"
    executable.write_text(
        """#!/bin/bash
set -e
echo "$*" >> "$NPM_CALL_LOG"
if [[ "$1" == "run" && "$2" == "build" ]]; then
  mkdir -p assets
  printf 'built from locked sources' > assets/security-cockpit.js
fi
"""
    )
    executable.chmod(0o755)
    return executable


def install_environment(
    tmp_path: Path,
    *,
    launchctl: Path,
    lsof: Path,
) -> dict[str, str]:
    home = tmp_path / "home"
    home.mkdir()
    return {
        "HOME": str(home),
        "CALL_LOG": str(tmp_path / "launchctl.log"),
        "STOCK_EVA_LAUNCHCTL": str(launchctl),
        "STOCK_EVA_LSOF": str(lsof),
        "STOCK_EVA_UV": str(fake_uv(tmp_path)),
        "STOCK_EVA_NPM": str(fake_npm(tmp_path)),
        "NPM_CALL_LOG": str(tmp_path / "npm.log"),
        "STOCK_EVA_UNAME": "Darwin",
    }


def stateful_install_environment(tmp_path: Path) -> dict[str, str]:
    environment = install_environment(
        tmp_path,
        launchctl=stateful_fake_launchctl(tmp_path),
        lsof=stateful_fake_lsof(tmp_path),
    )
    runtime_current = (
        Path(environment["HOME"]) / "Library/Application Support/Stock EVA/runtime/current"
    )
    launchctl_state = tmp_path / "launchctl-state"
    launchctl_state.touch()
    handoff_event_log = tmp_path / "handoff-events.log"
    handoff_event_log.touch()
    environment.update(
        {
            "LAUNCHCTL_STATE": str(launchctl_state),
            "HANDOFF_EVENT_LOG": str(handoff_event_log),
            "RUNTIME_CURRENT_PATH": str(runtime_current),
        }
    )
    return environment


def run_installer(
    project: Path,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            str(SCRIPTS / "stock_eva_launchagents_install.sh"),
            "--install",
            "--project-root",
            str(project),
        ],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def test_installer_uses_private_logs_and_succeeds_in_synthetic_home(
    tmp_path: Path,
) -> None:
    project = synthetic_project(tmp_path)
    launchctl = fake_launchctl(tmp_path)
    lsof = fake_lsof(tmp_path, occupied=False)
    environment = install_environment(
        tmp_path,
        launchctl=launchctl,
        lsof=lsof,
    )

    result = run_installer(project, environment)

    assert result.returncode == 0, result.stderr
    home = Path(environment["HOME"])
    installed = sorted((home / "Library/LaunchAgents").glob("*.plist"))
    assert len(installed) == 5
    log_root = home / "Library/Logs/Stock EVA"
    backup_root = home / "Library/Application Support/Stock EVA/backups"
    assert stat.S_IMODE(log_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(backup_root.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in log_root.iterdir())
    runtime = home / "Library/Application Support/Stock EVA/runtime"
    current = runtime / "current"
    config = home / "Library/Application Support/Stock EVA/config"
    data = home / "Library/Application Support/Stock EVA/data"
    assert current.is_symlink()
    assert (current / "backend/sentinel.txt").read_text() == "backend"
    assert (current / ".venv/bin/python").is_file()
    assert (current / "public/workspace/sentinel.txt").read_text() == "workspace"
    assert not (current / "public/.env").exists()
    assert not (current / "public/backend").exists()
    assert not (current / "public/var").exists()
    assert str(project) not in (installed[0]).read_text()
    assert str(current) in (installed[0]).read_text()
    runtime_env = (config / ".env").read_text()
    assert f"STOCK_EVA_MARKET_DATA_DIR={data / 'market'}" in runtime_env
    assert f"STOCK_EVA_USER_DATA_DIR={data / 'user'}" in runtime_env
    assert "STOCK_EVA_USER_DATABASE_NAME=stock_eva_user.sqlite3" in runtime_env
    assert "STOCK_EVA_PORTFOLIO_DATABASE_NAME=stock_eva_portfolio.sqlite3" in runtime_env
    assert f"STOCK_EVA_LOCAL_CONTROL_DIR={data / 'control'}" in runtime_env
    assert f"STOCK_EVA_LOCAL_MARKET_DATASET_ROOT={data / 'market-dataset'}" in runtime_env
    assert "STOCK_EVA_AKSHARE_SUPPLEMENTAL_ENABLED=false" in runtime_env
    assert "STOCK_EVA_SCHEDULED_REFRESH_ENABLED=true" in runtime_env
    assert (data / "control/control.sentinel").read_text() == "control"
    assert stat.S_IMODE(runtime.stat().st_mode) == 0o700
    assert stat.S_IMODE(config.stat().st_mode) == 0o700
    assert stat.S_IMODE((config / ".env").stat().st_mode) == 0o600
    assert stat.S_IMODE(data.stat().st_mode) == 0o700


def test_installer_builds_workspace_from_locked_sources(tmp_path: Path) -> None:
    project = synthetic_project(tmp_path)
    environment = install_environment(
        tmp_path,
        launchctl=fake_launchctl(tmp_path),
        lsof=fake_lsof(tmp_path, occupied=False),
    )

    result = run_installer(project, environment)

    assert result.returncode == 0, result.stderr
    calls = Path(environment["NPM_CALL_LOG"]).read_text().splitlines()
    assert calls == ["ci --ignore-scripts", "run build"]
    current = Path(environment["HOME"]) / "Library/Application Support/Stock EVA/runtime/current"
    assert (
        current / "public/workspace/assets/security-cockpit.js"
    ).read_text() == "built from locked sources"
    assert not (current / "public/workspace/node_modules").exists()


def test_installer_atomically_replaces_an_existing_runtime_symlink(
    tmp_path: Path,
) -> None:
    project = synthetic_project(tmp_path)
    environment = install_environment(
        tmp_path,
        launchctl=fake_launchctl(tmp_path),
        lsof=fake_lsof(tmp_path, occupied=False),
    )
    first = run_installer(project, environment)
    assert first.returncode == 0, first.stderr
    current = Path(environment["HOME"]) / "Library/Application Support/Stock EVA/runtime/current"
    first_target = current.readlink()

    (project / "uv.lock").write_text("synthetic lock v2")
    (project / "backend/sentinel.txt").write_text("backend-v2")
    second = run_installer(project, environment)

    assert second.returncode == 0, second.stderr
    assert current.is_symlink()
    assert current.readlink() != first_target
    assert (current / "backend/sentinel.txt").read_text() == "backend-v2"


def test_installer_stops_web_and_releases_8080_before_current_handoff(
    tmp_path: Path,
) -> None:
    project = synthetic_project(tmp_path)
    environment = stateful_install_environment(tmp_path)
    first = run_installer(project, environment)
    assert first.returncode == 0, first.stderr
    current = Path(environment["RUNTIME_CURRENT_PATH"])
    old_target = str(current.readlink())
    event_log = Path(environment["HANDOFF_EVENT_LOG"])
    event_log.write_text("")

    (project / "uv.lock").write_text("synthetic lock v2")
    (project / "backend/sentinel.txt").write_text("backend-v2")
    second = run_installer(project, environment)

    assert second.returncode == 0, second.stderr
    new_target = str(current.readlink())
    assert new_target != old_target
    events = event_log.read_text().splitlines()
    web_bootout = f"launchctl|bootout|com.finlay.stock-eva.web|{old_target}"
    released_8080 = f"lsof|8080|{old_target}|0"
    api_bootout = f"launchctl|bootout|com.finlay.stock-eva.api|{new_target}"
    assert events.count(web_bootout) == 1
    assert events.index(web_bootout) < events.index(released_8080)
    assert events.index(released_8080) < events.index(api_bootout)
    for label in (
        "com.finlay.stock-eva.api",
        "com.finlay.stock-eva.refresh",
        "com.finlay.stock-eva.calendar",
        "com.finlay.stock-eva.backup",
    ):
        assert events.count(f"launchctl|bootout|{label}|{new_target}") == 1
        assert events.count(f"launchctl|bootstrap|{label}|{new_target}") == 1


def test_installer_web_handoff_failure_restores_old_release_and_agents(
    tmp_path: Path,
) -> None:
    project = synthetic_project(tmp_path)
    environment = stateful_install_environment(tmp_path)
    first = run_installer(project, environment)
    assert first.returncode == 0, first.stderr
    current = Path(environment["RUNTIME_CURRENT_PATH"])
    old_target = str(current.readlink())
    agent_root = Path(environment["HOME"]) / "Library/LaunchAgents"
    previous_web_plist = agent_root / "com.finlay.stock-eva.web.plist"
    previous_web_plist.write_text("previous-web-plist")
    event_log = Path(environment["HANDOFF_EVENT_LOG"])
    event_log.write_text("")

    (project / "uv.lock").write_text("synthetic lock v2")
    (project / "backend/sentinel.txt").write_text("backend-v2")
    environment["STICKY_WEB_PORT"] = "1"
    second = run_installer(project, environment)

    assert second.returncode != 0
    assert "port 8080 remained occupied" in second.stderr
    assert "rollback: restored previous LaunchAgent state" in second.stderr
    assert str(current.readlink()) == old_target
    assert (current / "backend/sentinel.txt").read_text() == "backend"
    assert previous_web_plist.read_text() == "previous-web-plist"
    releases = current.parent / "releases"
    assert sorted(path.name for path in releases.iterdir()) == [Path(old_target).name]
    expected_labels = sorted(
        path.name.removesuffix(".plist.in") for path in LAUNCHD.glob("*.plist.in")
    )
    loaded_labels = sorted(Path(environment["LAUNCHCTL_STATE"]).read_text().splitlines())
    assert loaded_labels == expected_labels
    events = event_log.read_text().splitlines()
    assert f"launchctl|bootout|com.finlay.stock-eva.web|{old_target}" in events
    observed_targets = {
        parts[3] if parts[0] == "launchctl" else parts[2]
        for event in events
        if (parts := event.split("|"))[0] in ("launchctl", "lsof")
    }
    assert observed_targets <= {"", old_target}
    assert f"launchctl|bootstrap|com.finlay.stock-eva.web|{old_target}" in events


@pytest.mark.parametrize(
    ("faults", "missing_label", "failure_phase"),
    [
        pytest.param(
            {"FAIL_LABEL": "com.finlay.stock-eva.calendar"},
            "com.finlay.stock-eva.calendar",
            "bootstrap",
            id="rollback-bootstrap-fails",
        ),
        pytest.param(
            {
                "FAIL_ONCE_LABEL": "com.finlay.stock-eva.calendar",
                "BOOTSTRAP_NO_LOAD_LABEL": "com.finlay.stock-eva.web",
            },
            "com.finlay.stock-eva.web",
            "verify_loaded",
            id="rollback-bootstrap-never-loads",
        ),
    ],
)
def test_installer_reports_incomplete_rollback_when_loaded_agent_is_missing(
    tmp_path: Path,
    faults: dict[str, str],
    missing_label: str,
    failure_phase: str,
) -> None:
    project = synthetic_project(tmp_path)
    environment = stateful_install_environment(tmp_path)
    first = run_installer(project, environment)
    assert first.returncode == 0, first.stderr
    current = Path(environment["RUNTIME_CURRENT_PATH"])
    old_target = str(current.readlink())
    app_support = Path(environment["HOME"]) / "Library/Application Support/Stock EVA"
    config = app_support / "config/.env"
    previous_config = config.read_text()
    agent_root = Path(environment["HOME"]) / "Library/LaunchAgents"
    previous_calendar_plist = agent_root / "com.finlay.stock-eva.calendar.plist"
    previous_calendar_plist.write_text("previous-calendar-plist")

    (project / "uv.lock").write_text("synthetic lock v2")
    (project / "backend/sentinel.txt").write_text("backend-v2")
    (project / ".env").write_text((project / ".env").read_text() + "STOCK_EVA_TEST_MARKER=new\n")
    environment.update(faults)
    second = run_installer(project, environment)

    assert second.returncode == 9
    assert "rollback incomplete" in second.stderr
    assert f"{missing_label}:{failure_phase}" in second.stderr
    assert "rollback: restored previous LaunchAgent state" not in second.stderr
    assert str(current.readlink()) == old_target
    assert (current / "backend/sentinel.txt").read_text() == "backend"
    assert config.read_text() == previous_config
    assert previous_calendar_plist.read_text() == "previous-calendar-plist"
    releases = app_support / "runtime/releases"
    assert sorted(path.name for path in releases.iterdir()) == [Path(old_target).name]
    expected_loaded = {
        path.name.removesuffix(".plist.in") for path in LAUNCHD.glob("*.plist.in")
    } - {missing_label}
    actual_loaded = set(Path(environment["LAUNCHCTL_STATE"]).read_text().splitlines())
    assert actual_loaded == expected_loaded


def test_installer_rejects_manual_port_conflict_before_writing_plists(
    tmp_path: Path,
) -> None:
    project = synthetic_project(tmp_path)
    environment = install_environment(
        tmp_path,
        launchctl=fake_launchctl(tmp_path),
        lsof=fake_lsof(tmp_path, occupied=True),
    )

    result = run_installer(project, environment)

    assert result.returncode != 0
    assert "port 8000" in result.stderr
    assert not (Path(environment["HOME"]) / "Library/LaunchAgents").exists()


def test_installer_rolls_back_partial_bootstrap_failure(tmp_path: Path) -> None:
    project = synthetic_project(tmp_path)
    launchctl = fake_launchctl(tmp_path)
    environment = install_environment(
        tmp_path,
        launchctl=launchctl,
        lsof=fake_lsof(tmp_path, occupied=False),
    )
    environment["FAIL_LABEL"] = "com.finlay.stock-eva.calendar"

    result = run_installer(project, environment)

    assert result.returncode != 0
    agent_root = Path(environment["HOME"]) / "Library/LaunchAgents"
    assert list(agent_root.glob("com.finlay.stock-eva.*.plist")) == []
    assert "rollback: restored previous LaunchAgent state" in result.stderr
    app_support = Path(environment["HOME"]) / "Library/Application Support/Stock EVA"
    assert not (app_support / "runtime/current").exists()
    assert list((app_support / "runtime/releases").glob("*")) == []
    assert not (app_support / "config/.env").exists()


def test_installer_rollback_restores_preexisting_loaded_agent(
    tmp_path: Path,
) -> None:
    project = synthetic_project(tmp_path)
    launchctl = fake_launchctl(tmp_path)
    environment = install_environment(
        tmp_path,
        launchctl=launchctl,
        lsof=fake_lsof(tmp_path, occupied=False),
    )
    environment["LOADED_LABEL"] = "com.finlay.stock-eva.api"
    environment["FAIL_LABEL"] = "com.finlay.stock-eva.calendar"
    agent_root = Path(environment["HOME"]) / "Library/LaunchAgents"
    agent_root.mkdir(parents=True)
    previous = agent_root / "com.finlay.stock-eva.api.plist"
    previous.write_text("previous-api-plist")

    result = run_installer(project, environment)

    assert result.returncode != 0
    assert previous.read_text() == "previous-api-plist"
    assert sorted(path.name for path in agent_root.glob("*.plist")) == [
        "com.finlay.stock-eva.api.plist"
    ]


def test_installer_rejects_backup_path_override_it_cannot_honor(
    tmp_path: Path,
) -> None:
    project = synthetic_project(tmp_path, user_dir="custom/user")
    environment = install_environment(
        tmp_path,
        launchctl=fake_launchctl(tmp_path),
        lsof=fake_lsof(tmp_path, occupied=False),
    )

    result = run_installer(project, environment)

    assert result.returncode != 0
    assert "STOCK_EVA_USER_DATA_DIR=var/user" in result.stderr


def test_installer_rejects_portfolio_database_name_override_before_install(
    tmp_path: Path,
) -> None:
    project = synthetic_project(tmp_path)
    with (project / ".env").open("a") as environment_file:
        environment_file.write("STOCK_EVA_PORTFOLIO_DATABASE_NAME=elsewhere.sqlite3\n")
    environment = install_environment(
        tmp_path,
        launchctl=fake_launchctl(tmp_path),
        lsof=fake_lsof(tmp_path, occupied=False),
    )

    result = run_installer(project, environment)

    assert result.returncode != 0
    assert "STOCK_EVA_PORTFOLIO_DATABASE_NAME=stock_eva_portfolio.sqlite3" in result.stderr
    assert not (Path(environment["HOME"]) / "Library/LaunchAgents").exists()


def test_uninstaller_removes_only_named_agents_and_preserves_data(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    agent_root = home / "Library/LaunchAgents"
    logs = home / "Library/Logs/Stock EVA"
    backups = home / "Library/Application Support/Stock EVA/backups"
    agent_root.mkdir(parents=True)
    logs.mkdir(parents=True)
    backups.mkdir(parents=True)
    for template in LAUNCHD.glob("*.plist.in"):
        (agent_root / template.name.removesuffix(".in")).write_text("managed")
    unrelated = agent_root / "com.example.unrelated.plist"
    unrelated.write_text("preserve")
    (logs / "api.log").write_text("preserve")
    (backups / "snapshot.sqlite3").write_text("preserve")
    environment = {
        "HOME": str(home),
        "CALL_LOG": str(tmp_path / "launchctl.log"),
        "STOCK_EVA_LAUNCHCTL": str(fake_launchctl(tmp_path)),
    }

    result = subprocess.run(
        [
            str(SCRIPTS / "stock_eva_launchagents_uninstall.sh"),
            "--uninstall",
        ],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert unrelated.read_text() == "preserve"
    assert sorted(agent_root.glob("com.finlay.stock-eva.*.plist")) == []
    assert (logs / "api.log").read_text() == "preserve"
    assert (backups / "snapshot.sqlite3").read_text() == "preserve"
