#!/usr/bin/env bash
# 作业生命周期脚本的共享环境变量。
# 由 submit_job.sh / savepoint.sh / cancel_job.sh / restore_from_savepoint.sh source。
#
# 在执行 make 目标前，可覆盖任意变量，例如：
#   FLINK_REST=http://flink-master:8081 make submit-flink
#
# 对于生产部署，建议将真实值持久化到 /etc/orcp/flink.env，
# 再通过 env.local.sh（已在 .gitignore 中排除）进行 source。

set -euo pipefail

# --- Flink 集群 -----------------------------------------------------------
: "${FLINK_REST:=http://node-1:8081}"
: "${FLINK_JOB_JAR:=orcp-flink-job/target/orcp-flink-job.jar}"
: "${FLINK_JOB_MAIN_CLASS:=com.orcp.flink.OrcpFlinkJob}"
: "${FLINK_JOB_PARALLELISM:=2}"
: "${FLINK_SAVEPOINT_DIR:=file:///data/flink/savepoints}"
: "${FLINK_CONSUMER_GROUP:=orcp-flink-job}"

# --- Kafka / MySQL / OceanBase 提交时覆盖参数（可选）--------------------
# 未设置时不转发给作业；若设置，submit_job.sh 会以 --key value 形式传递。
: "${KAFKA_BOOTSTRAP:=}"
: "${ORCP_MID_TOPIC:=}"
: "${MYSQL_URL:=}"
: "${MYSQL_USER:=}"
: "${MYSQL_PASSWORD:=}"
: "${OB_URL:=}"
: "${OB_USER:=}"
: "${OB_PASSWORD:=}"

# --- 可选的站点本地覆盖 ---------------------------------------------------
if [[ -f "$(dirname "${BASH_SOURCE[0]}")/env.local.sh" ]]; then
    # shellcheck disable=SC1091
    source "$(dirname "${BASH_SOURCE[0]}")/env.local.sh"
fi

export FLINK_REST FLINK_JOB_JAR FLINK_JOB_MAIN_CLASS FLINK_JOB_PARALLELISM \
       FLINK_SAVEPOINT_DIR FLINK_CONSUMER_GROUP \
       KAFKA_BOOTSTRAP ORCP_MID_TOPIC \
       MYSQL_URL MYSQL_USER MYSQL_PASSWORD \
       OB_URL OB_USER OB_PASSWORD
