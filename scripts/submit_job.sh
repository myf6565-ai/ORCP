#!/usr/bin/env bash
# Stage E+ : upload orcp-flink-job.jar to the Flink cluster and run it.
# Skeleton placeholder - real implementation lands in Stage E.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

echo "[submit_job] TODO Stage E: POST ${FLINK_JOB_JAR} -> ${FLINK_REST}/jars/upload"
echo "[submit_job] TODO Stage E: POST ${FLINK_REST}/jars/<id>/run with parallelism=${FLINK_JOB_PARALLELISM}"
exit 0
