#!/usr/bin/env bash
# tail -F all cluster logs with per-line source prefixes.
# Ctrl-C to exit; sub-tails get SIGTERM via trap.
set -euo pipefail

LOG_DIR="${BALLISTA_LOCAL_WORK_DIR:-${HOME}/workspace/ballista-work}/logs"

if [ ! -d "$LOG_DIR" ] || [ -z "$(ls -A "$LOG_DIR"/*.log 2>/dev/null)" ]; then
    echo "no logs at ${LOG_DIR} — run up.sh first" >&2
    exit 1
fi

pids=()
cleanup() {
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

for f in "$LOG_DIR"/*.log; do
    prefix=$(basename "$f" .log)
    # sed -u for unbuffered line output; tail -F to follow rotation.
    tail -F "$f" 2>/dev/null | sed -u "s|^|[${prefix}] |" &
    pids+=($!)
done

wait
