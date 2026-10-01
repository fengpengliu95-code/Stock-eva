#!/bin/bash
set -euo pipefail

MODE=check
PROJECT_ROOT=""
REUSE_LOCAL_DATASET=0

usage() {
  echo "Usage: $0 [--check|--install] [--reuse-local-dataset] [--project-root ABSOLUTE_PATH]"
}

while (($#)); do
  case "$1" in
    --check)
      MODE=check
      ;;
    --install)
      MODE=install
      ;;
    --reuse-local-dataset)
      REUSE_LOCAL_DATASET=1
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

if [[ "$REUSE_LOCAL_DATASET" == "1" && "$MODE" != "install" ]]; then
  echo "error: --reuse-local-dataset is supported only with --install" >&2
  exit 2
fi

SYSTEM_NAME="${STOCK_EVA_UNAME:-$(uname -s)}"
LAUNCHCTL="${STOCK_EVA_LAUNCHCTL:-/bin/launchctl}"
LSOF="${STOCK_EVA_LSOF:-/usr/sbin/lsof}"
UV="${STOCK_EVA_UV:-$(command -v uv || true)}"
NPM="${STOCK_EVA_NPM:-$(command -v npm || true)}"

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
SOURCE_PYTHON="$PROJECT_ROOT/.venv/bin/python"
ENV_FILE="$PROJECT_ROOT/.env"
LAUNCH_AGENT_ROOT="$HOME/Library/LaunchAgents"
LOG_ROOT="$HOME/Library/Logs/Stock EVA"
APP_SUPPORT_ROOT="$HOME/Library/Application Support/Stock EVA"
RUNTIME_ROOT="$APP_SUPPORT_ROOT/runtime"
RELEASES_ROOT="$RUNTIME_ROOT/releases"
RUNTIME_CURRENT="$RUNTIME_ROOT/current"
CONFIG_ROOT="$APP_SUPPORT_ROOT/config"
DATA_ROOT="$APP_SUPPORT_ROOT/data"
PYTHON="$RUNTIME_CURRENT/.venv/bin/python"
PUBLIC_ROOT="$RUNTIME_CURRENT/public"
BACKUP_ROOT="$APP_SUPPORT_ROOT/backups"
DOMAIN="gui/$UID"
LABELS=(
  com.finlay.stock-eva.api
  com.finlay.stock-eva.web
  com.finlay.stock-eva.refresh
  com.finlay.stock-eva.calendar
  com.finlay.stock-eva.backup
)
WEB_LABEL=com.finlay.stock-eva.web
MARKET_CONTROL_LABELS=(
  com.finlay.stock-eva.api
  com.finlay.stock-eva.refresh
  com.finlay.stock-eva.calendar
)

if [[ ! -x "$SOURCE_PYTHON" ]]; then
  echo "error: missing project Python at $SOURCE_PYTHON; run 'uv sync --extra dev' first" >&2
  exit 1
fi
if [[ -z "$UV" || ! -x "$UV" ]]; then
  echo "error: uv executable is required to build an isolated runtime" >&2
  exit 1
fi
if [[ -z "$NPM" || ! -x "$NPM" ]]; then
  echo "error: npm executable is required to build the workspace" >&2
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
if /usr/bin/grep -q '^STOCK_EVA_USER_DATA_DIR=' "$ENV_FILE"; then
  CONFIGURED_USER_DATA_DIR="$(
    /usr/bin/awk -F= '$1 == "STOCK_EVA_USER_DATA_DIR" { print substr($0, index($0, "=") + 1); exit }' \
      "$ENV_FILE"
  )"
  if [[ "$CONFIGURED_USER_DATA_DIR" != "var/user" \
    && "$CONFIGURED_USER_DATA_DIR" != "var/user/" \
    && "$CONFIGURED_USER_DATA_DIR" != "$DATA_ROOT/user" \
    && "$CONFIGURED_USER_DATA_DIR" != "$DATA_ROOT/user/" ]]; then
    echo "error: LaunchAgent backup requires STOCK_EVA_USER_DATA_DIR=var/user" >&2
    exit 1
  fi
fi
if /usr/bin/grep -q '^STOCK_EVA_USER_DATABASE_NAME=' "$ENV_FILE" \
  && ! /usr/bin/grep -Eq \
    '^STOCK_EVA_USER_DATABASE_NAME=stock_eva_user.sqlite3$' \
    "$ENV_FILE"; then
  echo "error: LaunchAgent backup requires STOCK_EVA_USER_DATABASE_NAME=stock_eva_user.sqlite3" >&2
  exit 1
fi
if /usr/bin/grep -q '^STOCK_EVA_PORTFOLIO_DATABASE_NAME=' "$ENV_FILE" \
  && ! /usr/bin/grep -Eq \
    '^STOCK_EVA_PORTFOLIO_DATABASE_NAME=stock_eva_portfolio.sqlite3$' \
    "$ENV_FILE"; then
  echo "error: LaunchAgent backup requires STOCK_EVA_PORTFOLIO_DATABASE_NAME=stock_eva_portfolio.sqlite3" >&2
  exit 1
fi

RUNTIME_ASSETS=(backend workspace dashboard docs index.html pyproject.toml uv.lock README.md)
for relative in "${RUNTIME_ASSETS[@]}"; do
  if [[ ! -e "$PROJECT_ROOT/$relative" ]]; then
    echo "error: missing runtime asset $PROJECT_ROOT/$relative" >&2
    exit 1
  fi
done
if /usr/bin/git -C "$PROJECT_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  RELEASE_ID="$(/usr/bin/git -C "$PROJECT_ROOT" rev-parse HEAD)"
  RELEASE_FROM_GIT=1
else
  RELEASE_ID="synthetic-$(/usr/bin/shasum -a 256 "$PROJECT_ROOT/uv.lock" | /usr/bin/awk '{print substr($1,1,16)}')"
  RELEASE_FROM_GIT=0
fi
RELEASE_ROOT="$RELEASES_ROOT/$RELEASE_ID"

TEMP_ROOT="$(/usr/bin/mktemp -d "${TMPDIR:-/tmp}/stock-eva-launchd.XXXXXX")"
RUNTIME_STAGE=""
cleanup() {
  /bin/rm -rf "$TEMP_ROOT"
  if [[ -n "$RUNTIME_STAGE" && -e "$RUNTIME_STAGE" ]]; then
    /bin/rm -rf "$RUNTIME_STAGE"
  fi
}
trap cleanup EXIT

escape_sed() {
  /usr/bin/sed 's/[&|]/\\&/g' <<<"$1"
}

CONFIG_ESCAPED="$(escape_sed "$CONFIG_ROOT")"
PUBLIC_ESCAPED="$(escape_sed "$PUBLIC_ROOT")"
DATA_ESCAPED="$(escape_sed "$DATA_ROOT")"
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
    -e "s|__CONFIG_ROOT__|$CONFIG_ESCAPED|g" \
    -e "s|__PUBLIC_ROOT__|$PUBLIC_ESCAPED|g" \
    -e "s|__DATA_ROOT__|$DATA_ESCAPED|g" \
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
PRESERVE_REFRESH_UNLOADED=0
if [[ -f "$PREVIOUS_ROOT/existed.com.finlay.stock-eva.refresh" \
  && ! -f "$PREVIOUS_ROOT/loaded.com.finlay.stock-eva.refresh" ]]; then
  PRESERVE_REFRESH_UNLOADED=1
fi

port_is_listening() {
  "$LSOF" -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

wait_for_unloaded() {
  local label="$1"
  for _attempt in {1..50}; do
    if ! "$LAUNCHCTL" print "$DOMAIN/$label" >/dev/null 2>&1; then
      return 0
    fi
    /bin/sleep 0.1
  done
  return 1
}

wait_for_loaded() {
  local label="$1"
  for _attempt in {1..50}; do
    if "$LAUNCHCTL" print "$DOMAIN/$label" >/dev/null 2>&1; then
      return 0
    fi
    /bin/sleep 0.1
  done
  return 1
}

wait_for_port_release() {
  local port="$1"
  for _attempt in 1 2 3 4 5 6 7 8 9 10; do
    if ! port_is_listening "$port"; then
      return 0
    fi
    /bin/sleep 0.1
  done
  return 1
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
  echo "runtime=$RUNTIME_CURRENT"
  echo "release=$RELEASE_ID"
  echo "config=$CONFIG_ROOT"
  echo "data=$DATA_ROOT"
  echo "env=$ENV_FILE"
  echo "mutation=false"
  echo "install with: $0 --install"
  exit 0
fi

MUTATION_STARTED=0
AGENT_STATE_MUTATED=0
CURRENT_SWAPPED=0
PREVIOUS_CURRENT_TARGET=""
RELEASE_CREATED=0
CONFIG_CHANGED=0
rollback() {
  prior_status=$?
  status="${1:-$prior_status}"
  local -a restore_failures=()
  trap - ERR
  set +e
  if [[ "$MUTATION_STARTED" == "1" ]]; then
    if [[ "$AGENT_STATE_MUTATED" == "1" || "$CURRENT_SWAPPED" == "1" ]]; then
      for label in "${LABELS[@]}"; do
        "$LAUNCHCTL" bootout "$DOMAIN/$label" >/dev/null 2>&1 || true
      done
      for label in "${LABELS[@]}"; do
        wait_for_unloaded "$label" || true
      done
      for label in "${LABELS[@]}"; do
        installed="$LAUNCH_AGENT_ROOT/$label.plist"
        if [[ -f "$PREVIOUS_ROOT/existed.$label" ]]; then
          /bin/cp "$PREVIOUS_ROOT/$label.plist" "$installed"
        else
          /bin/rm -f "$installed"
        fi
      done
    fi
    if [[ "$CURRENT_SWAPPED" == "1" ]]; then
      /bin/rm -f "$RUNTIME_CURRENT"
      if [[ -n "$PREVIOUS_CURRENT_TARGET" ]]; then
        /bin/ln -s "$PREVIOUS_CURRENT_TARGET" "$RUNTIME_CURRENT"
      fi
    fi
    if [[ "$RELEASE_CREATED" == "1" && -d "$RELEASE_ROOT" ]]; then
      /bin/rm -rf "$RELEASE_ROOT"
    fi
    if [[ "$CONFIG_CHANGED" == "1" ]]; then
      if [[ -f "$CONFIG_ROOT/.env.previous" ]]; then
        /bin/cp "$CONFIG_ROOT/.env.previous" "$CONFIG_ROOT/.env"
        /bin/chmod 0600 "$CONFIG_ROOT/.env"
      else
        /bin/rm -f "$CONFIG_ROOT/.env"
      fi
    fi
    for label in "${LABELS[@]}"; do
      if [[ -f "$PREVIOUS_ROOT/loaded.$label" ]]; then
        if ! "$LAUNCHCTL" bootstrap \
          "$DOMAIN" \
          "$LAUNCH_AGENT_ROOT/$label.plist" >/dev/null 2>&1; then
          restore_failures+=("$label:bootstrap")
        elif ! wait_for_loaded "$label"; then
          restore_failures+=("$label:verify_loaded")
        fi
      fi
    done
    if ((${#restore_failures[@]} == 0)); then
      echo "rollback: restored previous LaunchAgent state" >&2
    else
      echo "rollback incomplete: ${restore_failures[*]}" >&2
    fi
  fi
  exit "$status"
}
trap rollback ERR

MUTATION_STARTED=1
/bin/mkdir -p \
  "$APP_SUPPORT_ROOT" \
  "$RUNTIME_ROOT" \
  "$RELEASES_ROOT"
/bin/chmod 0700 \
  "$APP_SUPPORT_ROOT" \
  "$RUNTIME_ROOT" \
  "$RELEASES_ROOT"

if [[ ! -f "$RELEASE_ROOT/.ready" ]]; then
  /bin/rm -rf "$RELEASE_ROOT"
  RUNTIME_STAGE="$RUNTIME_ROOT/.release-stage.$$"
  /bin/rm -rf "$RUNTIME_STAGE"
  /bin/mkdir -p "$RUNTIME_STAGE"
  if [[ "$RELEASE_FROM_GIT" == "1" ]]; then
    /usr/bin/git -C "$PROJECT_ROOT" archive \
      --format=tar \
      "$RELEASE_ID" \
      -- "${RUNTIME_ASSETS[@]}" \
      | /usr/bin/tar -xf - -C "$RUNTIME_STAGE"
  else
    for relative in "${RUNTIME_ASSETS[@]}"; do
      /usr/bin/ditto "$PROJECT_ROOT/$relative" "$RUNTIME_STAGE/$relative"
    done
  fi
  (
    cd "$RUNTIME_STAGE/workspace"
    "$NPM" ci --ignore-scripts
    "$NPM" run build
  )
  /bin/rm -rf \
    "$RUNTIME_STAGE/workspace/node_modules" \
    "$RUNTIME_STAGE/workspace/src"
  /bin/rm -f \
    "$RUNTIME_STAGE/workspace/package.json" \
    "$RUNTIME_STAGE/workspace/package-lock.json" \
    "$RUNTIME_STAGE/workspace/tsconfig.json" \
    "$RUNTIME_STAGE/workspace/vite.config.ts"
  /bin/mkdir -p "$RUNTIME_STAGE/public"
  for relative in index.html workspace dashboard docs; do
    /bin/mv "$RUNTIME_STAGE/$relative" "$RUNTIME_STAGE/public/$relative"
  done
  UV_PROJECT_ENVIRONMENT="$RUNTIME_STAGE/.venv" \
    "$UV" sync \
      --project "$RUNTIME_STAGE" \
      --frozen \
      --no-dev \
      --no-editable \
      --compile-bytecode
  if /usr/bin/find "$RUNTIME_STAGE/.venv" -name '*.pth' -type f -exec \
    /usr/bin/grep -IlF "$PROJECT_ROOT" {} + | /usr/bin/grep -q .; then
    echo "error: production runtime still references the development project" >&2
    false
  fi
  (
    cd "$TEMP_ROOT"
    "$RUNTIME_STAGE/.venv/bin/python" -c \
      "import backend, duckdb, fastapi, uvicorn"
  )
  LOCK_SHA="$(/usr/bin/shasum -a 256 "$RUNTIME_STAGE/uv.lock" | /usr/bin/awk '{print $1}')"
  BUILT_AT="$(/bin/date -u '+%Y-%m-%dT%H:%M:%SZ')"
  /usr/bin/printf \
    '{"git_sha":"%s","uv_lock_sha256":"%s","built_at":"%s","version":"0.1.0"}\n' \
    "$RELEASE_ID" \
    "$LOCK_SHA" \
    "$BUILT_AT" \
    >"$RUNTIME_STAGE/RELEASE.json"
  /usr/bin/touch "$RUNTIME_STAGE/.ready"
  /bin/chmod 0700 "$RUNTIME_STAGE"
  /bin/mv "$RUNTIME_STAGE" "$RELEASE_ROOT"
  RUNTIME_STAGE=""
  RELEASE_CREATED=1
fi

if [[ "$REUSE_LOCAL_DATASET" == "1" ]]; then
  # Prove the existing local immutable copy with the candidate runtime before
  # writing config/data/control state or stopping any LaunchAgent.  This is a
  # read-only gate: a failed proof only removes a newly-built candidate
  # release in rollback and must leave the installed runtime untouched.
  "$RELEASE_ROOT/.venv/bin/python" \
    -m backend.app.storage.mirror \
    --destination "$DATA_ROOT/market-dataset" \
    --verify-only
  echo "dataset: verified local reuse; copied_bytes=0"
fi

/bin/mkdir -p \
  "$LAUNCH_AGENT_ROOT" \
  "$LOG_ROOT" \
  "$BACKUP_ROOT" \
  "$CONFIG_ROOT" \
  "$DATA_ROOT"
/bin/chmod 0700 \
  "$LOG_ROOT" \
  "$BACKUP_ROOT" \
  "$CONFIG_ROOT" \
  "$DATA_ROOT"

DATA_ROOT_ESCAPED="$(escape_sed "$DATA_ROOT")"
/usr/bin/sed \
  -e "s|^STOCK_EVA_MARKET_DATA_DIR=.*$|STOCK_EVA_MARKET_DATA_DIR=$DATA_ROOT_ESCAPED/market|" \
  -e "s|^STOCK_EVA_USER_DATA_DIR=.*$|STOCK_EVA_USER_DATA_DIR=$DATA_ROOT_ESCAPED/user|" \
  -e "s|^STOCK_EVA_USER_DATABASE_NAME=.*$|STOCK_EVA_USER_DATABASE_NAME=stock_eva_user.sqlite3|" \
  -e "s|^STOCK_EVA_PORTFOLIO_DATABASE_NAME=.*$|STOCK_EVA_PORTFOLIO_DATABASE_NAME=stock_eva_portfolio.sqlite3|" \
  -e "s|^STOCK_EVA_LOCAL_CONTROL_DIR=.*$|STOCK_EVA_LOCAL_CONTROL_DIR=$DATA_ROOT_ESCAPED/control|" \
  -e "s|^STOCK_EVA_LOCAL_STAGING_DIR=.*$|STOCK_EVA_LOCAL_STAGING_DIR=$DATA_ROOT_ESCAPED/staging|" \
  -e "s|^STOCK_EVA_LOCAL_LOCK_DIR=.*$|STOCK_EVA_LOCAL_LOCK_DIR=$DATA_ROOT_ESCAPED/locks|" \
  -e "s|^STOCK_EVA_LOCAL_TEMP_DIR=.*$|STOCK_EVA_LOCAL_TEMP_DIR=$DATA_ROOT_ESCAPED/tmp|" \
  -e "s|^STOCK_EVA_LOCAL_MARKET_DATASET_ROOT=.*$|STOCK_EVA_LOCAL_MARKET_DATASET_ROOT=$DATA_ROOT_ESCAPED/market-dataset|" \
  -e "s|^STOCK_EVA_AKSHARE_SUPPLEMENTAL_ENABLED=.*$|STOCK_EVA_AKSHARE_SUPPLEMENTAL_ENABLED=false|" \
  -e "s|^STOCK_EVA_SCHEDULED_REFRESH_ENABLED=.*$|STOCK_EVA_SCHEDULED_REFRESH_ENABLED=true|" \
  "$ENV_FILE" >"$CONFIG_ROOT/.env.next"
while IFS='|' read -r key value; do
  if ! /usr/bin/grep -q "^$key=" "$CONFIG_ROOT/.env.next"; then
    echo "$key=$value" >>"$CONFIG_ROOT/.env.next"
  fi
done <<EOF
STOCK_EVA_MARKET_DATA_DIR|$DATA_ROOT/market
STOCK_EVA_USER_DATA_DIR|$DATA_ROOT/user
STOCK_EVA_USER_DATABASE_NAME|stock_eva_user.sqlite3
STOCK_EVA_PORTFOLIO_DATABASE_NAME|stock_eva_portfolio.sqlite3
STOCK_EVA_LOCAL_CONTROL_DIR|$DATA_ROOT/control
STOCK_EVA_LOCAL_STAGING_DIR|$DATA_ROOT/staging
STOCK_EVA_LOCAL_LOCK_DIR|$DATA_ROOT/locks
STOCK_EVA_LOCAL_TEMP_DIR|$DATA_ROOT/tmp
STOCK_EVA_LOCAL_MARKET_DATASET_ROOT|$DATA_ROOT/market-dataset
STOCK_EVA_AKSHARE_SUPPLEMENTAL_ENABLED|false
STOCK_EVA_SCHEDULED_REFRESH_ENABLED|true
EOF
/bin/chmod 0600 "$CONFIG_ROOT/.env.next"
if [[ -f "$CONFIG_ROOT/.env" ]]; then
  /bin/cp "$CONFIG_ROOT/.env" "$CONFIG_ROOT/.env.previous"
  /bin/chmod 0600 "$CONFIG_ROOT/.env.previous"
fi
/bin/mv -f "$CONFIG_ROOT/.env.next" "$CONFIG_ROOT/.env"
CONFIG_CHANGED=1

for name in market user control staging locks tmp; do
  destination="$DATA_ROOT/$name"
  /bin/mkdir -p "$destination"
  /bin/chmod 0700 "$destination"
  source="$PROJECT_ROOT/var/$name"
  if [[ -d "$source" ]] \
    && [[ -z "$(/usr/bin/find "$destination" -mindepth 1 -print -quit)" ]]; then
    /usr/bin/ditto "$source" "$destination"
  fi
done
/bin/chmod 0700 "$DATA_ROOT"

AGENT_STATE_MUTATED=1
for label in "${MARKET_CONTROL_LABELS[@]}"; do
  if [[ -f "$PREVIOUS_ROOT/loaded.$label" ]]; then
    "$LAUNCHCTL" bootout "$DOMAIN/$label" >/dev/null
  fi
done
for label in "${MARKET_CONTROL_LABELS[@]}"; do
  if [[ -f "$PREVIOUS_ROOT/loaded.$label" ]] \
    && ! wait_for_unloaded "$label"; then
    echo "error: market control service remained loaded before schema migration" >&2
    false
  fi
done
if ! wait_for_port_release 8000; then
  echo "error: market API port remained occupied before schema migration" >&2
  false
fi

set +e
(
  cd "$CONFIG_ROOT"
  "$RELEASE_ROOT/.venv/bin/python" \
    -m backend.app.cli market-schema-migrate \
    >/dev/null 2>&1
)
SCHEMA_MIGRATION_STATUS=$?
set -e
if [[ "$SCHEMA_MIGRATION_STATUS" -ne 0 ]]; then
  echo "error: market control schema migration failed" >&2
  rollback "$SCHEMA_MIGRATION_STATUS"
fi

if [[ "$REUSE_LOCAL_DATASET" != "1" ]]; then
  "$RELEASE_ROOT/.venv/bin/python" \
    -m backend.app.storage.mirror \
    --source /Volumes/Stock/stock-eva-market \
    --destination "$DATA_ROOT/market-dataset" \
    --execute
fi

if [[ -L "$RUNTIME_CURRENT" ]]; then
  PREVIOUS_CURRENT_TARGET="$(/usr/bin/readlink "$RUNTIME_CURRENT")"
elif [[ -e "$RUNTIME_CURRENT" ]]; then
  echo "error: runtime current pointer is not a symbolic link" >&2
  false
fi

if [[ -f "$PREVIOUS_ROOT/loaded.$WEB_LABEL" ]]; then
  AGENT_STATE_MUTATED=1
  "$LAUNCHCTL" bootout "$DOMAIN/$WEB_LABEL" >/dev/null
  if ! wait_for_unloaded "$WEB_LABEL"; then
    echo "error: $WEB_LABEL remained loaded before runtime handoff" >&2
    false
  fi
fi
if ! wait_for_port_release 8080; then
  echo "error: port 8080 remained occupied before runtime handoff" >&2
  false
fi

NEXT_CURRENT="$RUNTIME_ROOT/.current.$$"
/bin/rm -f "$NEXT_CURRENT"
/bin/ln -s "releases/$RELEASE_ID" "$NEXT_CURRENT"
# BSD mv follows a destination symlink to a directory unless -h is explicit.
/bin/mv -fh "$NEXT_CURRENT" "$RUNTIME_CURRENT"
CURRENT_SWAPPED=1

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
  if [[ "$label" != "$WEB_LABEL" \
    && "$label" != "com.finlay.stock-eva.api" \
    && "$label" != "com.finlay.stock-eva.refresh" \
    && "$label" != "com.finlay.stock-eva.calendar" \
    && -f "$PREVIOUS_ROOT/loaded.$label" ]]; then
    "$LAUNCHCTL" bootout "$DOMAIN/$label" >/dev/null
  fi
done
AGENT_STATE_MUTATED=1
for label in "${LABELS[@]}"; do
  if [[ "$label" != "$WEB_LABEL" \
    && "$label" != "com.finlay.stock-eva.api" \
    && "$label" != "com.finlay.stock-eva.refresh" \
    && "$label" != "com.finlay.stock-eva.calendar" \
    && -f "$PREVIOUS_ROOT/loaded.$label" ]] \
    && ! wait_for_unloaded "$label"; then
    echo "error: $label remained loaded after bootout" >&2
    false
  fi
done

for port in 8000 8080; do
  if ! wait_for_port_release "$port"; then
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
  if [[ "$label" == "com.finlay.stock-eva.refresh" \
    && "$PRESERVE_REFRESH_UNLOADED" == "1" ]]; then
    continue
  fi
  "$LAUNCHCTL" bootstrap \
    "$DOMAIN" \
    "$LAUNCH_AGENT_ROOT/$label.plist"
done

MUTATION_STARTED=0
CURRENT_SWAPPED=0
RELEASE_CREATED=0
CONFIG_CHANGED=0
for legacy in .venv backend workspace dashboard docs index.html pyproject.toml uv.lock README.md .env; do
  legacy_path="$RUNTIME_ROOT/$legacy"
  if [[ -e "$legacy_path" && "$legacy_path" != "$RUNTIME_CURRENT" ]]; then
    /bin/rm -rf "$legacy_path"
  fi
done
for release in "$RELEASES_ROOT"/*; do
  [[ -d "$release" ]] || continue
  relative="releases/$(basename "$release")"
  if [[ "$relative" != "releases/$RELEASE_ID" \
    && "$relative" != "$PREVIOUS_CURRENT_TARGET" ]]; then
    /bin/rm -rf "$release"
  fi
done
trap - ERR
echo "installed: 5 Stock EVA LaunchAgents"
if [[ "$PRESERVE_REFRESH_UNLOADED" == "1" ]]; then
  echo "refresh state: preserved unloaded"
fi
echo "runtime: $RUNTIME_CURRENT"
echo "release: $RELEASE_ID"
echo "config: $CONFIG_ROOT"
echo "data: $DATA_ROOT"
echo "status: $PROJECT_ROOT/scripts/stock_eva_launchagents_status.sh"
