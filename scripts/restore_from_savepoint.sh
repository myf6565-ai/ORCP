#!/usr/bin/env bash
# Stage E+ : restart a job by pointing to a savepoint path.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

SAVEPOINT_PATH="${1:-}"
if [[ -z "${SAVEPOINT_PATH}" ]]; then
    echo "usage: $0 <savepoint-path>" >&2
    exit 1
fi
echo "[restore] TODO Stage E: POST ${FLINK_REST}/jars/<id>/run savepointPath=${SAVEPOINT_PATH}"
exit 0
