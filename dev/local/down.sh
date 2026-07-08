#!/usr/bin/env bash
# Cleanly shut down a local Ballista cluster brought up by up.sh.
#
#   - SIGTERM by PID file first
#   - falls back to SIGKILL by process name for survivors
#   - verifies via `ps` — only returns success when nothing's alive
#
# Ends the exit-code-144 confusion permanently: this script actually checks.
set -euo pipefail

WORK_DIR="${BALLISTA_LOCAL_WORK_DIR:-${HOME}/workspace/ballista-work}"
PID_FILE="${WORK_DIR}/pids"

# SIGTERM known PIDs.
if [ -f "$PID_FILE" ]; then
    while read -r pid; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    sleep 1
fi

# Escalate to SIGKILL for any survivors matching the process name.
# `pgrep -f X` exits 1 on no match, and `set -o pipefail` treats that as
# a script failure, so wrap the call in a group with `|| true`.
survivors=$({ pgrep -f 'ballista-(scheduler|executor)'; } || true)
if [ -n "$survivors" ]; then
    echo "→ escalating to SIGKILL: $(echo $survivors)"
    echo "$survivors" | xargs kill -9 2>/dev/null || true
    sleep 1
fi

# Verify.
remaining=$({ pgrep -cf 'ballista-(scheduler|executor)'; } || true)
remaining=${remaining:-0}
if [ "$remaining" -gt 0 ]; then
    echo "$remaining ballista processes still alive after SIGKILL" >&2
    ps -eo pid,args | grep -E 'ballista-(scheduler|executor)' | grep -v grep >&2
    exit 1
fi

: > "$PID_FILE" 2>/dev/null || true
echo "→ cluster down."
