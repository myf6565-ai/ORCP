#!/usr/bin/env bash
# Shared environment for job lifecycle scripts. Source this file from
# submit_job.sh / savepoint.sh / cancel_job.sh / restore_from_savepoint.sh.
#
# Override any of these before invoking make targets, e.g.:
#   FLINK_REST=http://flink-master:8081 make submit-flink
#
# For real deployments, persist the production values in
# /etc/orcp/flink.env and source it from env.local.sh (gitignored).

set -euo pipefail

# --- Flink cluster ---------------------------------------------------------
: "${FLINK_REST:=http://node-1:8081}"
: "${FLINK_JOB_JAR:=orcp-flink-job/target/orcp-flink-job.jar}"
: "${FLINK_JOB_MAIN_CLASS:=com.orcp.flink.OrcpFlinkJob}"
: "${FLINK_JOB_PARALLELISM:=2}"
: "${FLINK_SAVEPOINT_DIR:=file:///data/flink/savepoints}"
: "${FLINK_CONSUMER_GROUP:=orcp-flink-job}"

# --- Kafka / MySQL / OceanBase (optional submit-time overrides) -----------
# Unset by default; if set, submit_job.sh forwards them as --key value args
# to the job and they override job.properties.
: "${KAFKA_BOOTSTRAP:=}"
: "${ORCP_MID_TOPIC:=}"
: "${MYSQL_URL:=}"
: "${MYSQL_USER:=}"
: "${MYSQL_PASSWORD:=}"
: "${OB_URL:=}"
: "${OB_USER:=}"
: "${OB_PASSWORD:=}"

# --- Optional site-local overrides ----------------------------------------
if [[ -f "$(dirname "${BASH_SOURCE[0]}")/env.local.sh" ]]; then
    # shellcheck disable=SC1091
    source "$(dirname "${BASH_SOURCE[0]}")/env.local.sh"
fi

export FLINK_REST FLINK_JOB_JAR FLINK_JOB_MAIN_CLASS FLINK_JOB_PARALLELISM \
       FLINK_SAVEPOINT_DIR FLINK_CONSUMER_GROUP \
       KAFKA_BOOTSTRAP ORCP_MID_TOPIC \
       MYSQL_URL MYSQL_USER MYSQL_PASSWORD \
       OB_URL OB_USER OB_PASSWORD
