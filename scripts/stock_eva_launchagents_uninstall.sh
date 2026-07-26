#!/bin/bash
set -euo pipefail

MODE=check
LABELS=(
  com.finlay.stock-eva.api
  com.finlay.stock-eva.web
  com.finlay.stock-eva.refresh
  com.finlay.stock-eva.calendar
  com.finlay.stock-eva.backup
)
LAUNCH_AGENT_ROOT="$HOME/Library/LaunchAgents"
DOMAIN="gui/$UID"

case "${1:---check}" in
  --check)
    MODE=check
    ;;
  --uninstall)
    MODE=uninstall
    ;;
  *)
    echo "Usage: $0 [--check|--uninstall]" >&2
    exit 2
    ;;
esac

if [[ "$MODE" == "check" ]]; then
  echo "would remove only these LaunchAgents:"
  printf '  %s\n' "${LABELS[@]}"
  echo "logs, backups, .env, databases and NAS data are preserved"
  echo "mutation=false"
  exit 0
fi

for label in "${LABELS[@]}"; do
  /bin/launchctl bootout "$DOMAIN/$label" >/dev/null 2>&1 || true
  /bin/rm -f "$LAUNCH_AGENT_ROOT/$label.plist"
done

echo "uninstalled: Stock EVA LaunchAgents"
echo "preserved: logs, backups, .env, local databases and NAS data"
