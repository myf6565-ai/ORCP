#!/usr/bin/env bash
#
# 触发一个正在运行的 Flink 作业的 savepoint，并等待完成。
# 将最终 savepoint 路径打印到 stdout（可直接管道给 restore_from_savepoint.sh）。
#
# 用法：
#   savepoint.sh <job-id>
#   JOB=<id> make savepoint
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

JOB_ID="${1:-${JOB:-}}"
if [[ -z "${JOB_ID}" ]]; then
    echo "用法：$0 <job-id>" >&2
    exit 1
fi

echo "[savepoint] 为作业 ${JOB_ID} 触发 savepoint -> ${FLINK_SAVEPOINT_DIR}" >&2
trigger_resp=$(curl --fail --silent --show-error \
    -X POST -H 'Content-Type: application/json' \
    -d "{\"target-directory\":\"${FLINK_SAVEPOINT_DIR}\",\"cancel-job\":false}" \
    "${FLINK_REST}/jobs/${JOB_ID}/savepoints")
request_id=$(echo "${trigger_resp}" | sed -n 's/.*"request-id":"\([^"]*\)".*/\1/p')
if [[ -z "${request_id}" ]]; then
    echo "错误：响应中无 request-id：${trigger_resp}" >&2
    exit 2
fi
echo "[savepoint] request-id=${request_id}，轮询中..." >&2

# 轮询直到 savepoint 完成或失败（最多约 5 分钟）。
for _ in $(seq 1 150); do
    sleep 2
    status_resp=$(curl --fail --silent --show-error \
        "${FLINK_REST}/jobs/${JOB_ID}/savepoints/${request_id}")
    state=$(echo "${status_resp}" | sed -n 's/.*"status":{"id":"\([^"]*\)".*/\1/p')
    case "${state}" in
        COMPLETED)
            loc=$(echo "${status_resp}" | sed -n 's/.*"location":"\([^"]*\)".*/\1/p')
            echo "[savepoint] 完成：${loc}" >&2
            echo "${loc}"   # 打印到 stdout 供调用方捕获
            exit 0
            ;;
        IN_PROGRESS|"") ;;
        *)
            echo "错误：savepoint 状态异常 '${state}'：${status_resp}" >&2
            exit 3
            ;;
    esac
done

echo "错误：等待 savepoint 请求 ${request_id} 超时" >&2
exit 4
