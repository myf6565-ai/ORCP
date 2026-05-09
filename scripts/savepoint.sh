#!/usr/bin/env bash
#
# Triggers a savepoint for a running job and waits until it completes.
# Prints the final savepoint path on stdout (for piping into restore).
#
# Usage:  savepoint.sh <job-id>
#         JOB=<id> make savepoint
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

JOB_ID="${1:-${JOB:-}}"
if [[ -z "${JOB_ID}" ]]; then
    echo "usage: $0 <job-id>" >&2
    exit 1
fi

echo "[savepoint] triggering for job=${JOB_ID} -> ${FLINK_SAVEPOINT_DIR}" >&2
trigger_resp=$(curl --fail --silent --show-error \
    -X POST -H 'Content-Type: application/json' \
    -d "{\"target-directory\":\"${FLINK_SAVEPOINT_DIR}\",\"cancel-job\":false}" \
    "${FLINK_REST}/jobs/${JOB_ID}/savepoints")
request_id=$(echo "${trigger_resp}" | sed -n 's/.*"request-id":"\([^"]*\)".*/\1/p')
if [[ -z "${request_id}" ]]; then
    echo "error: no request-id in response: ${trigger_resp}" >&2
    exit 2
fi
echo "[savepoint] request-id=${request_id}; polling..." >&2

# Poll until the savepoint reports COMPLETED or FAILED (up to ~5 minutes).
for _ in $(seq 1 150); do
    sleep 2
    status_resp=$(curl --fail --silent --show-error \
        "${FLINK_REST}/jobs/${JOB_ID}/savepoints/${request_id}")
    state=$(echo "${status_resp}" | sed -n 's/.*"status":{"id":"\([^"]*\)".*/\1/p')
    case "${state}" in
        COMPLETED)
            # operation.location is the path of the completed savepoint.
            loc=$(echo "${status_resp}" | sed -n 's/.*"location":"\([^"]*\)".*/\1/p')
            echo "[savepoint] completed: ${loc}" >&2
            echo "${loc}"
            exit 0
            ;;
        IN_PROGRESS|"") ;;
        *)
            echo "error: unexpected savepoint state '${state}': ${status_resp}" >&2
            exit 3
            ;;
    esac
done

echo "error: timed out waiting for savepoint request ${request_id}" >&2
exit 4
