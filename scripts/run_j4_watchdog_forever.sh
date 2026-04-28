#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs/instansi-filter-shards"
mkdir -p "$LOG_DIR"

INTERVAL="${INTERVAL:-300}"
MAX_PARALLEL="${MAX_PARALLEL:-6}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"
CHILD_TIMEOUT="${CHILD_TIMEOUT:-300}"
TIMEOUT="${TIMEOUT:-45}"
STALE_MINUTES="${STALE_MINUTES:-15}"
RESTART_DELAY="${RESTART_DELAY:-10}"

while :; do
  cd "$ROOT_DIR"
  python3 -u scripts/watch_j4_instansi_filters.py \
    --interval "$INTERVAL" \
    --max-parallel "$MAX_PARALLEL" \
    --max-attempts "$MAX_ATTEMPTS" \
    --child-timeout "$CHILD_TIMEOUT" \
    --timeout "$TIMEOUT" \
    --restart-stale \
    --stale-minutes "$STALE_MINUTES" \
    >> "$LOG_DIR/watchdog-daemon.log" 2>&1
  sleep "$RESTART_DELAY"
done
