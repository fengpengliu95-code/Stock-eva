#!/bin/bash
set -euo pipefail

MODE=check
PROJECT_ROOT=""

usage() {
  echo "Usage: $0 [--check|--install] [--project-root ABSOLUTE_PATH]"
}

while (($#)); do
  case "$1" in
    --check)
      MODE=check
      ;;
    --install)
      MODE=install
      ;;
    --project-root)
      shift
      PROJECT_ROOT="${1:-}"
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
  shift
done

SYSTEM_NAME="${STOCK_EVA_UNAME:-$(uname -s)}"
LAUNCHCTL="${STOCK_EVA_LAUNCHCTL:-/bin/launchctl}"
LSOF="${STOCK_EVA_LSOF:-/usr/sbin/lsof}"

if [[ "$SYSTEM_NAME" != "Darwin" ]]; then
  echo "error: LaunchAgents are supported only on macOS" >&2
  exit 1
fi

if [[ -z "$PROJECT_ROOT" ]]; then
  PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
elif [[ "$PROJECT_ROOT" != /* ]]; then
  echo "error: --project-root must be absolute" >&2
  exit 2
else
  PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd -P)"
fi

TEMPLATE_ROOT="$PROJECT_ROOT/launchd"
PYTHON="$PROJECT_ROOT/.venv/bin/python"
ENV_FILE="$PROJECT_ROOT/.env"
LAUNCH_AGENT_ROOT="$HOME/Library/LaunchAgents"
LOG_ROOT="$HOME/Library/Logs/Stock EVA"
BACKUP_ROOT="$HOME/Library/Application Support/Stock EVA/backups"
DOMAIN="gui/$UID"
LABELS=(
  com.finlay.stock-eva.api
  com.finlay.stock-eva.web
  com.finlay.stock-eva.refresh
  com.finlay.stock-eva.calendar
  com.finlay.stock-eva.backup
)

if [[ ! -x "$PYTHON" ]]; then
  echo "error: missing project Python at $PYTHON; run 'uv sync --extra dev' first" >&2
  exit 1
fi
if [[ ! -f "$ENV_FILE" ]]; then
  echo "error: missing ignored local config $ENV_FILE" >&2
  echo "copy .env.example to .env and configure the NAS dataset path" >&2
  exit 1
fi
if ! /usr/bin/grep -Eq \
  '^STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/stock-eva-market/?$' \
  "$ENV_FILE"; then
  echo "error: .env must point STOCK_EVA_NAS_MARKET_DATASET_ROOT to /Volumes/Stock/stock-eva-market" >&2
  exit 1
fi
if ! /usr/bin/grep -Eq '^STOCK_EVA_AUTO_REFRESH_ENABLED=false$' "$ENV_FILE"; then
  echo "error: .env must keep STOCK_EVA_AUTO_REFRESH_ENABLED=false" >&2
  echo "the LaunchAgent one-shot schedule owns refresh execution" >&2
  exit 1
fi
if /usr/bin/grep -q '^STOCK_EVA_USER_DATA_DIR=' "$ENV_FILE" \
  && ! /usr/bin/grep -Eq '^STOCK_EVA_USER_DATA_DIR=var/user/?$' "$ENV_FILE"; then
  echo "error: LaunchAgent backup requires STOCK_EVA_USER_DATA_DIR=var/user" >&2
  exit 1
fi
if /usr/bin/grep -q '^STOCK_EVA_USER_DATABASE_NAME=' "$ENV_FILE" \
  && ! /usr/bin/grep -Eq \
    '^STOCK_EVA_USER_DATABASE_NAME=stock_eva_user.sqlite3$' \
    "$ENV_FILE"; then
  echo "error: LaunchAgent backup requires STOCK_EVA_USER_DATABASE_NAME=stock_eva_user.sqlite3" >&2
  exit 1
fi

TEMP_ROOT="$(/usr/bin/mktemp -d "${TMPDIR:-/tmp}/stock-eva-launchd.XXXXXX")"
cleanup() {
  /bin/rm -rf "$TEMP_ROOT"
}
trap cleanup EXIT

escape_sed() {
  /usr/bin/sed 's/[&|]/\\&/g' <<<"$1"
}

PROJECT_ESCAPED="$(escape_sed "$PROJECT_ROOT")"
PYTHON_ESCAPED="$(escape_sed "$PYTHON")"
LOG_ESCAPED="$(escape_sed "$LOG_ROOT")"
BACKUP_ESCAPED="$(escape_sed "$BACKUP_ROOT")"

for label in "${LABELS[@]}"; do
  template="$TEMPLATE_ROOT/$label.plist.in"
  rendered="$TEMP_ROOT/$label.plist"
  if [[ ! -f "$template" ]]; then
    echo "error: missing template $template" >&2
    exit 1
  fi
  /usr/bin/sed \
    -e "s|__PROJECT_ROOT__|$PROJECT_ESCAPED|g" \
    -e "s|__PYTHON__|$PYTHON_ESCAPED|g" \
    -e "s|__LOG_DIR__|$LOG_ESCAPED|g" \
    -e "s|__BACKUP_ROOT__|$BACKUP_ESCAPED|g" \
    "$template" >"$rendered"
  /usr/bin/plutil -lint "$rendered" >/dev/null
  if /usr/bin/grep -q '__[A-Z_][A-Z_]*__' "$rendered"; then
    echo "error: unresolved template token in $label" >&2
    exit 1
  fi
done

PREVIOUS_ROOT="$TEMP_ROOT/previous"
/bin/mkdir -p "$PREVIOUS_ROOT"
for label in "${LABELS[@]}"; do
  installed="$LAUNCH_AGENT_ROOT/$label.plist"
  if [[ -f "$installed" ]]; then
    /bin/cp "$installed" "$PREVIOUS_ROOT/$label.plist"
    /usr/bin/touch "$PREVIOUS_ROOT/existed.$label"
  fi
  if "$LAUNCHCTL" print "$DOMAIN/$label" >/dev/null 2>&1; then
    /usr/bin/touch "$PREVIOUS_ROOT/loaded.$label"
  fi
done

port_is_listening() {
  "$LSOF" -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

if port_is_listening 8000 \
  && [[ ! -f "$PREVIOUS_ROOT/loaded.com.finlay.stock-eva.api" ]]; then
  echo "error: port 8000 is already in use by a non-Stock-EVA LaunchAgent process" >&2
  exit 1
fi
if port_is_listening 8080 \
  && [[ ! -f "$PREVIOUS_ROOT/loaded.com.finlay.stock-eva.web" ]]; then
  echo "error: port 8080 is already in use by a non-Stock-EVA LaunchAgent process" >&2
  exit 1
fi

if [[ "$MODE" == "check" ]]; then
  echo "ready: 5 LaunchAgent templates rendered and validated"
  echo "project=$PROJECT_ROOT"
  echo "env=$ENV_FILE"
  echo "mutation=false"
  echo "install with: $0 --install"
  exit 0
fi

MUTATION_STARTED=0
rollback() {
  status=$?
  trap - ERR
  set +e
  if [[ "$MUTATION_STARTED" == "1" ]]; then
    for label in "${LABELS[@]}"; do
      "$LAUNCHCTL" bootout "$DOMAIN/$label" >/dev/null 2>&1 || true
    done
    for label in "${LABELS[@]}"; do
      installed="$LAUNCH_AGENT_ROOT/$label.plist"
      if [[ -f "$PREVIOUS_ROOT/existed.$label" ]]; then
        /bin/cp "$PREVIOUS_ROOT/$label.plist" "$installed"
      else
        /bin/rm -f "$installed"
      fi
    done
    for label in "${LABELS[@]}"; do
      if [[ -f "$PREVIOUS_ROOT/loaded.$label" ]]; then
        "$LAUNCHCTL" bootstrap \
          "$DOMAIN" \
          "$LAUNCH_AGENT_ROOT/$label.plist" >/dev/null 2>&1 || true
      fi
    done
    echo "rollback: restored previous LaunchAgent state" >&2
  fi
  exit "$status"
}
trap rollback ERR

MUTATION_STARTED=1
/bin/mkdir -p "$LAUNCH_AGENT_ROOT" "$LOG_ROOT" "$BACKUP_ROOT"
/bin/chmod 0700 "$LOG_ROOT" "$BACKUP_ROOT"
LOG_FILES=(
  api.log api-error.log
  web.log web-error.log
  refresh.log refresh-error.log
  calendar.log calendar-error.log
  backup.log backup-error.log
)
for log_name in "${LOG_FILES[@]}"; do
  /usr/bin/touch "$LOG_ROOT/$log_name"
  /bin/chmod 0600 "$LOG_ROOT/$log_name"
done

for label in "${LABELS[@]}"; do
  if [[ -f "$PREVIOUS_ROOT/loaded.$label" ]]; then
    "$LAUNCHCTL" bootout "$DOMAIN/$label" >/dev/null
  fi
done

for port in 8000 8080; do
  for _attempt in 1 2 3 4 5 6 7 8 9 10; do
    if ! port_is_listening "$port"; then
      break
    fi
    /bin/sleep 0.1
  done
  if port_is_listening "$port"; then
    echo "error: port $port remained occupied after previous agents stopped" >&2
    false
  fi
done

for label in "${LABELS[@]}"; do
  /usr/bin/install -m 0644 \
    "$TEMP_ROOT/$label.plist" \
    "$LAUNCH_AGENT_ROOT/$label.plist"
done
for label in "${LABELS[@]}"; do
  "$LAUNCHCTL" bootstrap \
    "$DOMAIN" \
    "$LAUNCH_AGENT_ROOT/$label.plist"
done

MUTATION_STARTED=0
trap - ERR
echo "installed: 5 Stock EVA LaunchAgents"
echo "status: $PROJECT_ROOT/scripts/stock_eva_launchagents_status.sh"
