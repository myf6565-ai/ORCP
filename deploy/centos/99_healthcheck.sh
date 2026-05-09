#!/usr/bin/env bash
#
# 99_healthcheck.sh --- ORCP 基础设施端到端健康检查。
# 可在任意节点运行；对每个子系统输出 PASS/FAIL，遇到第一个失败时以非零退出，
# 可作为阶段 C 部署的前置门控。

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

load_cluster_env

FAIL=0
check() {
    local label="$1"; shift
    if "$@" >/dev/null 2>&1; then
        printf '  [ OK ] %s\n' "${label}"
    else
        printf '  [FAIL] %s\n' "${label}"
        FAIL=1
    fi
}

echo "ORCP 基础设施健康检查 @ $(date -Iseconds)，主机：$(hostname)"
echo "集群信息：NODE_ID=${NODE_ID}  NODE_COUNT=${NODE_COUNT}"
echo

echo "== 基础环境 =="
check "已安装 JDK 8"               bash -c '[[ -x /opt/jdk-8/bin/java ]] && /opt/jdk-8/bin/java -version 2>&1 | grep -q "1.8"'
check "orcp 用户存在"               id orcp
check "时区 = Asia/Shanghai"        bash -c "timedatectl | grep -q 'Time zone: Asia/Shanghai'"

echo
echo "== ZooKeeper =="
for i in $(seq 1 "${NODE_COUNT}"); do
    host="$(host_for "${i}")"
    [[ -z "${host}" ]] && continue
    check "zk ruok ${host}:2181"      bash -c "echo ruok | nc -w 2 ${host} 2181 | grep -q '^imok$'"
done

echo
echo "== Kafka =="
for i in $(seq 1 "${NODE_COUNT}"); do
    host="$(host_for "${i}")"
    [[ -z "${host}" ]] && continue
    check "kafka TCP ${host}:9092"    bash -c "echo > /dev/tcp/${host}/9092"
done
if [[ -x /opt/kafka/bin/kafka-topics.sh ]]; then
    check "topic: orcp.src.demo"       bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server $(host_for 1):9092 --list | grep -qx orcp.src.demo"
    check "topic: orcp.mid.events"     bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server $(host_for 1):9092 --list | grep -qx orcp.mid.events"
fi

echo
echo "== Flink =="
check "JobManager 8081 可达"          curl -fsS "http://$(host_for 1):8081/overview"
check "TaskManager 已注册"            bash -c "curl -fsS http://$(host_for 1):8081/taskmanagers | grep -q '\"taskmanagers\"'"

echo
echo "== Nacos =="
check "Nacos 就绪"                    curl -fsS "http://$(host_for 1):8848/nacos/v1/console/health/readiness"

if [[ "${NODE_ID}" == "1" ]]; then
    echo
    echo "== MySQL（node-1）=="
    check "mysqld ping"               mysqladmin ping --silent
fi

echo
if [[ "${FAIL}" -eq 0 ]]; then
    echo "所有检查均通过。"
    exit 0
else
    echo "存在一项或多项检查失败。"
    exit 1
fi
