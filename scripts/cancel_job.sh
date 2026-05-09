#!/usr/bin/env bash
#
# 优雅取消正在运行的 Flink 作业（带 savepoint）。
# 使用 stop-with-savepoint 端点，作业会排空待处理记录后再保存状态，
# 适合计划内发布场景。
#
# 用法：
#   cancel_job.sh <job-id>
#   JOB=<id> make cancel
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

JOB_ID="${1:-${JOB:-}}"
if [[ -z "${JOB_ID}" ]]; then
    echo "用法：$0 <job-id>" >&2
    exit 1
fi

echo "[cancel] 对作业 ${JOB_ID} 执行 stop-with-savepoint -> ${FLINK_SAVEPOINT_DIR}"
resp=$(curl --fail --silent --show-error \
    -X POST -H 'Content-Type: application/json' \
    -d "{\"targetDirectory\":\"${FLINK_SAVEPOINT_DIR}\",\"drain\":false}" \
    "${FLINK_REST}/jobs/${JOB_ID}/stop")
request_id=$(echo "${resp}" | sed -n 's/.*"request-id":"\([^"]*\)".*/\1/p')
if [[ -z "${request_id}" ]]; then
    echo "错误：响应中无 request-id：${resp}" >&2
    exit 2
fi

echo "[cancel] 轮询 request-id=${request_id}..."
for _ in $(seq 1 150); do
    sleep 2
    status=$(curl --fail --silent --show-error \
        "${FLINK_REST}/jobs/${JOB_ID}/savepoints/${request_id}")
    state=$(echo "${status}" | sed -n 's/.*"status":{"id":"\([^"]*\)".*/\1/p')
    case "${state}" in
        COMPLETED)
            loc=$(echo "${status}" | sed -n 's/.*"location":"\([^"]*\)".*/\1/p')
            echo "[cancel] 作业已停止；savepoint 路径：${loc}"
            echo "${loc}"
            exit 0
            ;;
        IN_PROGRESS|"") ;;
        *)
            echo "错误：状态异常 '${state}'：${status}" >&2
            exit 3
            ;;
    esac
done
echo "错误：等待 stop-with-savepoint ${request_id} 超时" >&2
exit 4
