#!/usr/bin/env bash
#
# Gracefully cancels a running job with a savepoint.
# Uses the stop-with-savepoint endpoint so the job drains pending records
# before persisting its state, which is what you want for planned deploys.
#
# Usage:  cancel_job.sh <job-id>
#         JOB=<id> make cancel
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

JOB_ID="${1:-${JOB:-}}"
if [[ -z "${JOB_ID}" ]]; then
    echo "usage: $0 <job-id>" >&2
    exit 1
fi

echo "[cancel] stop-with-savepoint for ${JOB_ID} -> ${FLINK_SAVEPOINT_DIR}"
resp=$(curl --fail --silent --show-error \
    -X POST -H 'Content-Type: application/json' \
    -d "{\"targetDirectory\":\"${FLINK_SAVEPOINT_DIR}\",\"drain\":false}" \
    "${FLINK_REST}/jobs/${JOB_ID}/stop")
request_id=$(echo "${resp}" | sed -n 's/.*"request-id":"\([^"]*\)".*/\1/p')
if [[ -z "${request_id}" ]]; then
    echo "error: no request-id in response: ${resp}" >&2
    exit 2
fi

echo "[cancel] polling request-id=${request_id}..."
for _ in $(seq 1 150); do
    sleep 2
    status=$(curl --fail --silent --show-error \
        "${FLINK_REST}/jobs/${JOB_ID}/savepoints/${request_id}")
    state=$(echo "${status}" | sed -n 's/.*"status":{"id":"\([^"]*\)".*/\1/p')
    case "${state}" in
        COMPLETED)
            loc=$(echo "${status}" | sed -n 's/.*"location":"\([^"]*\)".*/\1/p')
            echo "[cancel] job stopped; savepoint: ${loc}"
            echo "${loc}"
            exit 0
            ;;
        IN_PROGRESS|"") ;;
        *)
            echo "error: unexpected state '${state}': ${status}" >&2
            exit 3
            ;;
    esac
done
echo "error: timed out waiting for stop-with-savepoint ${request_id}" >&2
exit 4
