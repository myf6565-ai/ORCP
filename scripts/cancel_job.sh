#!/usr/bin/env bash
# Stage E+ : gracefully cancel a running Flink job with savepoint.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

JOB_ID="${1:-${JOB:-}}"
if [[ -z "${JOB_ID}" ]]; then
    echo "usage: $0 <job-id>" >&2
    exit 1
fi
echo "[cancel_job] TODO Stage E: PATCH ${FLINK_REST}/jobs/${JOB_ID}?mode=cancel with savepoint -> ${FLINK_SAVEPOINT_DIR}"
exit 0
