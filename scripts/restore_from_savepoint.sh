#!/usr/bin/env bash
#
# Re-submits the job starting from a savepoint.  Thin wrapper that exports
# SAVEPOINT_PATH and delegates to submit_job.sh, which handles the upload +
# run REST calls.
#
# Usage:  restore_from_savepoint.sh <savepoint-path>
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SAVEPOINT_PATH="${1:-}"
if [[ -z "${SAVEPOINT_PATH}" ]]; then
    echo "usage: $0 <savepoint-path>" >&2
    echo "example: $0 file:///data/flink/savepoints/savepoint-abc123" >&2
    exit 1
fi

export SAVEPOINT_PATH
echo "[restore] delegating to submit_job.sh with SAVEPOINT_PATH=${SAVEPOINT_PATH}"
exec "${SCRIPT_DIR}/submit_job.sh"
