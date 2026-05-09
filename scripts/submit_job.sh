#!/usr/bin/env bash
#
# Uploads the fat jar to the JobManager's jar store and runs it.
# Optional env:
#   JAR_ID         reuse an already-uploaded jar-id (skips upload)
#   SAVEPOINT_PATH start from this savepoint instead of a fresh state
#   OB_USER / OB_PASSWORD / MYSQL_USER / ... override job.properties at submit time
#
# Exit codes:
#   0  job accepted by the JobManager
#   1  usage / pre-flight error
#   2  upload failed
#   3  run request failed
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

if [[ ! -f "${FLINK_JOB_JAR}" ]]; then
    echo "error: jar not found at ${FLINK_JOB_JAR}" >&2
    echo "build it first with: make build" >&2
    exit 1
fi

# --- upload (idempotent-ish: reuse JAR_ID if the caller provides one) -----
if [[ -z "${JAR_ID:-}" ]]; then
    echo "[submit] uploading ${FLINK_JOB_JAR} -> ${FLINK_REST}/jars/upload"
    upload_resp=$(curl --fail --silent --show-error \
        -X POST -H "Expect:" -F "jarfile=@${FLINK_JOB_JAR}" \
        "${FLINK_REST}/jars/upload") || {
            echo "error: jar upload failed" >&2
            exit 2
        }
    # Response: {"filename":"/.../flink-web-upload/<uuid>_orcp-flink-job.jar","status":"success"}
    JAR_ID="$(echo "${upload_resp}" | sed -n 's/.*"filename":"[^"]*\/\([^"/]*\.jar\)".*/\1/p')"
    if [[ -z "${JAR_ID}" ]]; then
        echo "error: could not parse jar id from response: ${upload_resp}" >&2
        exit 2
    fi
    echo "[submit] uploaded as jar-id=${JAR_ID}"
else
    echo "[submit] reusing existing jar-id=${JAR_ID}"
fi

# --- assemble run request ------------------------------------------------
# programArgs is a single string; JSON-escape the --key value flags we pick
# up from the environment.
program_args=""
append_arg() {
    local key="$1" val="${2:-}"
    [[ -z "${val}" ]] && return 0
    # Escape any embedded double quote in the value.
    val=${val//\"/\\\"}
    program_args+=" --${key} \"${val}\""
}
append_arg kafka.bootstrap       "${KAFKA_BOOTSTRAP:-}"
append_arg kafka.mid.topic       "${ORCP_MID_TOPIC:-}"
append_arg kafka.group.id        "${FLINK_CONSUMER_GROUP:-}"
append_arg mysql.url             "${MYSQL_URL:-}"
append_arg mysql.user            "${MYSQL_USER:-}"
append_arg mysql.password        "${MYSQL_PASSWORD:-}"
append_arg ob.url                "${OB_URL:-}"
append_arg ob.user               "${OB_USER:-}"
append_arg ob.password           "${OB_PASSWORD:-}"
append_arg parallelism           "${FLINK_JOB_PARALLELISM:-}"
program_args="${program_args# }"  # strip leading space

run_body=$(cat <<EOF
{
  "entryClass": "${FLINK_JOB_MAIN_CLASS}",
  "parallelism": ${FLINK_JOB_PARALLELISM},
  "programArgs": "${program_args}",
  "allowNonRestoredState": false$( [[ -n "${SAVEPOINT_PATH:-}" ]] && echo ",
  \"savepointPath\": \"${SAVEPOINT_PATH}\"" )
}
EOF
)

echo "[submit] POST ${FLINK_REST}/jars/${JAR_ID}/run"
echo "[submit] body: ${run_body}"
run_resp=$(curl --fail --silent --show-error \
    -X POST -H 'Content-Type: application/json' \
    -d "${run_body}" \
    "${FLINK_REST}/jars/${JAR_ID}/run") || {
        echo "error: run request failed" >&2
        exit 3
    }
echo "[submit] response: ${run_resp}"
echo "[submit] done. Inspect: ${FLINK_REST}/jobs/overview"
