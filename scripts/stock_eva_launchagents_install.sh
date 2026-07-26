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

if [[ "$(uname -s)" != "Darwin" ]]; then
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

if [[ "$MODE" == "check" ]]; then
  echo "ready: 5 LaunchAgent templates rendered and validated"
  echo "project=$PROJECT_ROOT"
  echo "env=$ENV_FILE"
  echo "mutation=false"
  echo "install with: $0 --install"
  exit 0
fi

/bin/mkdir -p "$LAUNCH_AGENT_ROOT" "$LOG_ROOT" "$BACKUP_ROOT"
for label in "${LABELS[@]}"; do
  /bin/launchctl bootout "$DOMAIN/$label" >/dev/null 2>&1 || true
  /usr/bin/install -m 0644 \
    "$TEMP_ROOT/$label.plist" \
    "$LAUNCH_AGENT_ROOT/$label.plist"
  /bin/launchctl bootstrap \
    "$DOMAIN" \
    "$LAUNCH_AGENT_ROOT/$label.plist"
done

echo "installed: 5 Stock EVA LaunchAgents"
echo "status: $PROJECT_ROOT/scripts/stock_eva_launchagents_status.sh"
