#!/usr/bin/env bash
#
# 将 fat jar 上传到 JobManager 的 jar 存储区并运行作业。
#
# 可选环境变量：
#   JAR_ID         重用已上传的 jar-id（跳过上传步骤）
#   SAVEPOINT_PATH 从该 savepoint 路径恢复状态（而非全新启动）
#   OB_USER / OB_PASSWORD / MYSQL_USER / ... 在提交时覆盖 job.properties 中的值
#
# 退出码：
#   0  作业已被 JobManager 接受
#   1  用法错误 / 前置检查失败
#   2  jar 上传失败
#   3  运行请求失败
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

if [[ ! -f "${FLINK_JOB_JAR}" ]]; then
    echo "错误：找不到 jar 文件：${FLINK_JOB_JAR}" >&2
    echo "请先执行：make build" >&2
    exit 1
fi

# --- 上传（如调用方提供了 JAR_ID，则跳过上传步骤）-----------------------
if [[ -z "${JAR_ID:-}" ]]; then
    echo "[submit] 上传 ${FLINK_JOB_JAR} -> ${FLINK_REST}/jars/upload"
    upload_resp=$(curl --fail --silent --show-error \
        -X POST -H "Expect:" -F "jarfile=@${FLINK_JOB_JAR}" \
        "${FLINK_REST}/jars/upload") || {
            echo "错误：jar 上传失败" >&2
            exit 2
        }
    # 响应格式：{"filename":"/.../flink-web-upload/<uuid>_orcp-flink-job.jar","status":"success"}
    JAR_ID="$(echo "${upload_resp}" | sed -n 's/.*"filename":"[^"]*\/\([^"/]*\.jar\)".*/\1/p')"
    if [[ -z "${JAR_ID}" ]]; then
        echo "错误：无法从响应中解析 jar-id：${upload_resp}" >&2
        exit 2
    fi
    echo "[submit] 已上传，jar-id=${JAR_ID}"
else
    echo "[submit] 复用已存在的 jar-id=${JAR_ID}"
fi

# --- 拼装运行请求 ----------------------------------------------------------
# programArgs 是一个字符串，Flink 使用自身的 shell 解析器分割键值对。
program_args=""
append_arg() {
    local key="$1" val="${2:-}"
    [[ -z "${val}" ]] && return 0
    val=${val//\"/\\\"}   # 转义值中的双引号
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
program_args="${program_args# }"  # 去除开头多余的空格

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
echo "[submit] 请求体：${run_body}"
run_resp=$(curl --fail --silent --show-error \
    -X POST -H 'Content-Type: application/json' \
    -d "${run_body}" \
    "${FLINK_REST}/jars/${JAR_ID}/run") || {
        echo "错误：运行请求失败" >&2
        exit 3
    }
echo "[submit] 响应：${run_resp}"
echo "[submit] 完成。查看作业：${FLINK_REST}/jobs/overview"
