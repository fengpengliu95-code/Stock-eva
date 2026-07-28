#!/bin/bash
set -u

LABELS=(
  com.finlay.stock-eva.api
  com.finlay.stock-eva.web
  com.finlay.stock-eva.refresh
  com.finlay.stock-eva.calendar
  com.finlay.stock-eva.backup
)
LAUNCH_AGENT_ROOT="$HOME/Library/LaunchAgents"
DOMAIN="gui/$UID"
missing=0

wait_for_http() {
  url="$1"
  attempt=1
  while [[ "$attempt" -le 15 ]]; do
    if /usr/bin/curl --fail --silent --max-time 1 "$url" >/dev/null; then
      return 0
    fi
    /bin/sleep 1
    attempt=$((attempt + 1))
  done
  return 1
}

for label in "${LABELS[@]}"; do
  if /bin/launchctl print "$DOMAIN/$label" >/dev/null 2>&1; then
    state=loaded
  else
    state=not-loaded
    missing=1
  fi
  if [[ -f "$LAUNCH_AGENT_ROOT/$label.plist" ]]; then
    installed=yes
  else
    installed=no
  fi
  echo "$label state=$state installed=$installed"
done

if wait_for_http http://127.0.0.1:8000/api/v1/health; then
  echo "api=ready url=http://127.0.0.1:8000/api/v1/health"
else
  echo "api=unavailable"
fi

if wait_for_http http://127.0.0.1:8080/workspace/; then
  echo "workspace=ready url=http://127.0.0.1:8080/workspace/"
else
  echo "workspace=unavailable"
fi

storage="$(
  /usr/bin/curl --fail --silent --max-time 2 \
    http://127.0.0.1:8000/api/v1/storage/readiness 2>/dev/null
)" || storage=""
if [[ -n "$storage" ]]; then
  echo "storage=$storage"
else
  echo "storage=unavailable"
fi

exit "$missing"
