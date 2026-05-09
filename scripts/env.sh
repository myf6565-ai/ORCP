#!/usr/bin/env bash
# Shared environment for job lifecycle scripts. Source this file from
# submit_job.sh / savepoint.sh / cancel_job.sh / restore_from_savepoint.sh.
#
# Override any of these before invoking make targets, e.g.:
#   FLINK_REST=http://flink-master:8081 make submit-flink

set -euo pipefail

: "${FLINK_REST:=http://node-1:8081}"
: "${FLINK_JOB_JAR:=orcp-flink-job/target/orcp-flink-job.jar}"
: "${FLINK_JOB_MAIN_CLASS:=com.orcp.flink.OrcpFlinkJob}"
: "${FLINK_JOB_PARALLELISM:=2}"
: "${FLINK_SAVEPOINT_DIR:=file:///data/flink/savepoints}"

export FLINK_REST FLINK_JOB_JAR FLINK_JOB_MAIN_CLASS FLINK_JOB_PARALLELISM FLINK_SAVEPOINT_DIR
