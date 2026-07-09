#!/usr/bin/env bash
# Bring up a local Ballista cluster for iterating on this branch.
#
#   - kills anything running from a previous `up` (idempotent)
#   - rebuilds scheduler + executor if source is newer than binary
#   - clears work dirs (previous-run spill is stale by definition)
#   - launches scheduler + 2 executors on real disk (NOT /tmp — that's tmpfs)
#   - waits for each port to bind before returning
#   - writes PIDs to ${WORK_DIR}/pids for down.sh
#
# Usage:  ./dev/local/up.sh
# Layout: scheduler on 50050, exec0 on 50051/50151, exec1 on 50052/50152
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="${BALLISTA_LOCAL_WORK_DIR:-${HOME}/workspace/ballista-work}"
LOG_DIR="${WORK_DIR}/logs"
PID_FILE="${WORK_DIR}/pids"
SCHEDULER_BIN="${REPO_ROOT}/target/debug/ballista-scheduler"
EXECUTOR_BIN="${REPO_ROOT}/target/debug/ballista-executor"

mkdir -p "${WORK_DIR}/exec0" "${WORK_DIR}/exec1" "${LOG_DIR}"

# Kill anything left over first — idempotent bring-up.
"$(dirname "${BASH_SOURCE[0]}")/down.sh"

# Rebuild if any source file is newer than the binary. Covers .rs and .proto
# (proto changes flow through build.rs into generated .rs, so proto-newer-
# than-binary is a real trigger).
needs_rebuild() {
    local bin=$1
    if [ ! -x "$bin" ]; then return 0; fi
    local newest_src
    newest_src=$(find "${REPO_ROOT}/ballista" \
        \( -name '*.rs' -o -name '*.proto' \) \
        -newer "$bin" -print -quit 2>/dev/null)
    [ -n "$newest_src" ]
}

if needs_rebuild "$SCHEDULER_BIN" || needs_rebuild "$EXECUTOR_BIN"; then
    echo "→ rebuilding stale binaries..."
    (cd "$REPO_ROOT" && cargo build \
        --bin ballista-scheduler --bin ballista-executor \
        --features build-binary \
        -p ballista-scheduler -p ballista-executor)
fi

# Fresh work-dirs.
rm -rf "${WORK_DIR}/exec0"/* "${WORK_DIR}/exec1"/*
: > "$PID_FILE"

wait_port() {
    local port=$1
    for _ in $(seq 1 60); do
        ss -tln 2>/dev/null | grep -q ":${port} " && return 0
        sleep 0.5
    done
    echo "port ${port} never bound" >&2
    return 1
}

echo "→ starting scheduler on 127.0.0.1:50050..."
RUST_LOG="${RUST_LOG:-info}" "$SCHEDULER_BIN" \
    --bind-host 127.0.0.1 --external-host 127.0.0.1 --bind-port 50050 \
    > "${LOG_DIR}/scheduler.log" 2>&1 &
echo $! >> "$PID_FILE"
wait_port 50050

for i in 0 1; do
    port=$((50051 + i))
    grpc=$((50151 + i))
    echo "→ starting executor $i on 127.0.0.1:${port}..."
    RUST_LOG="${RUST_LOG:-ballista=info}" "$EXECUTOR_BIN" \
        --scheduler-host 127.0.0.1 --scheduler-port 50050 \
        --bind-host 127.0.0.1 --external-host 127.0.0.1 \
        --bind-port "$port" --bind-grpc-port "$grpc" \
        --work-dir "${WORK_DIR}/exec${i}" \
        --concurrent-tasks 4 --memory-pool-size "${MEMORY_POOL_SIZE:-16GB}" \
        > "${LOG_DIR}/exec${i}.log" 2>&1 &
    echo $! >> "$PID_FILE"
    wait_port "$port"
done

echo "→ cluster up. PIDs: $(tr '\n' ' ' < "$PID_FILE")"
echo "→ logs: ./dev/local/tail.sh   (or tail -F ${LOG_DIR}/*.log)"
echo "→ query: ./dev/local/q.sh <N>"
