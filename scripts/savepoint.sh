#!/usr/bin/env bash
# Stage E+ : trigger a savepoint for a running Flink job and print its path.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

JOB_ID="${1:-${JOB:-}}"
if [[ -z "${JOB_ID}" ]]; then
    echo "usage: $0 <job-id>" >&2
    exit 1
fi
echo "[savepoint] TODO Stage E: POST ${FLINK_REST}/jobs/${JOB_ID}/savepoints target-directory=${FLINK_SAVEPOINT_DIR}"
exit 0
