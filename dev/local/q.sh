#!/usr/bin/env bash
# Run a single h2o window query against the local cluster.
#
#   ./q.sh 8                  # Q8 with defaults
#   ./q.sh 8 --iterations 3   # extra flags forwarded to h2o
#
# Runs with `timeout 120` so a livelock doesn't compound while you're afk.
# AQE is enabled by default so the ParallelWindow rule fires.
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <query_number> [extra h2o flags]" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
H2O_DATA="${H2O_DATA_DIR:-${HOME}/workspace/arrow-datafusion/benchmarks/data/h2o}"
H2O_BIN="${REPO_ROOT}/target/debug/h2o"
QUERIES="${REPO_ROOT}/benchmarks/queries/h2o/window.sql"

query=$1
shift

if [ ! -x "$H2O_BIN" ] || [ "${REPO_ROOT}/benchmarks/src/bin/h2o.rs" -nt "$H2O_BIN" ]; then
    echo "→ rebuilding h2o..."
    (cd "$REPO_ROOT" && cargo build --bin h2o -p ballista-benchmarks)
fi

exec timeout "${Q_TIMEOUT:-120}" "$H2O_BIN" ballista \
    --host 127.0.0.1 --port 50050 \
    --queries-path "$QUERIES" \
    --path unused \
    --join-paths "${H2O_DATA}/J1_1e7_NA_0.parquet,${H2O_DATA}/J1_1e7_1e1_0.parquet,${H2O_DATA}/J1_1e7_1e4_0.parquet,${H2O_DATA}/J1_1e7_1e7_NA.parquet" \
    --query "$query" \
    --partitions 8 \
    --iterations 1 \
    -c ballista.planner.adaptive.enabled=true \
    "$@"
