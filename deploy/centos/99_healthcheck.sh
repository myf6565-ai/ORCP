#!/usr/bin/env bash
#
# 99_healthcheck.sh --- End-to-end sanity check for the ORCP infrastructure.
# Run on any node; reports PASS/FAIL for each subsystem and exits non-zero on
# the first failure so this can gate a Stage C deploy.

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

echo "ORCP infra health check @ $(date -Iseconds) on $(hostname)"
echo "Cluster: NODE_ID=${NODE_ID}  NODE_COUNT=${NODE_COUNT}"
echo

echo "== Base =="
check "JDK 8 installed"               bash -c '[[ -x /opt/jdk-8/bin/java ]] && /opt/jdk-8/bin/java -version 2>&1 | grep -q "1.8"'
check "orcp user exists"              id orcp
check "timezone = Asia/Shanghai"      bash -c "timedatectl | grep -q 'Time zone: Asia/Shanghai'"

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
    check "kafka tcp ${host}:9092"    bash -c "echo > /dev/tcp/${host}/9092"
done
if [[ -x /opt/kafka/bin/kafka-topics.sh ]]; then
    check "orcp.src.demo topic"       bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server $(host_for 1):9092 --list | grep -qx orcp.src.demo"
    check "orcp.mid.events topic"     bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server $(host_for 1):9092 --list | grep -qx orcp.mid.events"
fi

echo
echo "== Flink =="
check "JobManager 8081 reachable"     curl -fsS "http://$(host_for 1):8081/overview"
check "TaskManagers registered"       bash -c "curl -fsS http://$(host_for 1):8081/taskmanagers | grep -q '\"taskmanagers\"'"

echo
echo "== Nacos =="
check "Nacos readiness"               curl -fsS "http://$(host_for 1):8848/nacos/v1/console/health/readiness"

if [[ "${NODE_ID}" == "1" ]]; then
    echo
    echo "== MySQL (node-1) =="
    check "mysqld ping"               mysqladmin ping --silent
fi

echo
if [[ "${FAIL}" -eq 0 ]]; then
    echo "All checks passed."
    exit 0
else
    echo "One or more checks failed."
    exit 1
fi
