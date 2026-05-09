#!/usr/bin/env bash
#
# 从指定 savepoint 路径重新提交作业。
# 设置 SAVEPOINT_PATH 后委托给 submit_job.sh 完成上传与运行的 REST 调用。
#
# 用法：
#   restore_from_savepoint.sh <savepoint-路径>
#   示例：restore_from_savepoint.sh file:///data/flink/savepoints/savepoint-abc123
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SAVEPOINT_PATH="${1:-}"
if [[ -z "${SAVEPOINT_PATH}" ]]; then
    echo "用法：$0 <savepoint-路径>" >&2
    echo "示例：$0 file:///data/flink/savepoints/savepoint-abc123" >&2
    exit 1
fi

export SAVEPOINT_PATH
echo "[restore] SAVEPOINT_PATH=${SAVEPOINT_PATH}，委托给 submit_job.sh"
exec "${SCRIPT_DIR}/submit_job.sh"
