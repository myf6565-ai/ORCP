#!/usr/bin/env bash
#
# 30_install_flink.sh --- Apache Flink 1.17.2 standalone 集群。
# 参见 DEV_SPEC §5.5。
#
# 节点角色：
#   NODE_ID == 1   -> JobManager（单节点 POC 时同时启动 TaskManager）
#   NODE_ID >= 2   -> TaskManager
#
# 连接器 jar 放在 /opt/flink/lib/ 中，SQL 客户端与 fat jar 均可共享。
# Prometheus reporter jar 从 opt/ 移动到 lib/ 以激活指标上报功能。

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

: "${FLINK_VERSION:=1.17.2}"
: "${FLINK_SCALA:=2.12}"
: "${FLINK_KAFKA_CONNECTOR_VERSION:=1.17.2}"
: "${FLINK_JDBC_CONNECTOR_VERSION:=3.1.2-1.17}"
: "${MYSQL_CONNECTOR_VERSION:=8.0.28}"
: "${OCEANBASE_CLIENT_VERSION:=2.4.14}"

FLINK_TARBALL="flink-${FLINK_VERSION}-bin-scala_${FLINK_SCALA}.tgz"
FLINK_URL="${ORCP_APACHE_MIRROR}/flink/flink-${FLINK_VERSION}/${FLINK_TARBALL}"

INSTALL_ROOT=/opt
FLINK_HOME_LINK="${INSTALL_ROOT}/flink"
FLINK_EXTRACT_DIR="${INSTALL_ROOT}/flink-${FLINK_VERSION}"

CHECKPOINT_DIR=/data/flink/checkpoints
SAVEPOINT_DIR=/data/flink/savepoints

# -------------------------------------------------------------------------
# 下载与目录布局
# -------------------------------------------------------------------------
if [[ ! -d "${FLINK_EXTRACT_DIR}" ]]; then
    log_info "下载 Flink ${FLINK_VERSION}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${FLINK_TARBALL}"
    download_to "${FLINK_URL}" "${TMP_TAR}"
    tar -xzf "${TMP_TAR}" -C "${INSTALL_ROOT}"
fi

rm -f "${FLINK_HOME_LINK}"
ln -s "${FLINK_EXTRACT_DIR}" "${FLINK_HOME_LINK}"

ensure_dir "${CHECKPOINT_DIR}" "${ORCP_USER}:${ORCP_GROUP}" 0755
ensure_dir "${SAVEPOINT_DIR}"  "${ORCP_USER}:${ORCP_GROUP}" 0755

# -------------------------------------------------------------------------
# 将连接器 jar 放入 lib/
# -------------------------------------------------------------------------
LIB_DIR="${FLINK_HOME_LINK}/lib"
declare -A JARS
JARS["flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"]="https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"
JARS["flink-connector-jdbc-${FLINK_JDBC_CONNECTOR_VERSION}.jar"]="https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/${FLINK_JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${FLINK_JDBC_CONNECTOR_VERSION}.jar"
JARS["mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"]="https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"
JARS["oceanbase-client-${OCEANBASE_CLIENT_VERSION}.jar"]="https://repo.maven.apache.org/maven2/com/oceanbase/oceanbase-client/${OCEANBASE_CLIENT_VERSION}/oceanbase-client-${OCEANBASE_CLIENT_VERSION}.jar"

for jar in "${!JARS[@]}"; do
    if [[ -f "${LIB_DIR}/${jar}" ]]; then
        log_info "已存在，跳过：${jar}"
        continue
    fi
    log_info "下载 ${jar}"
    download_to "${JARS[${jar}]}" "${LIB_DIR}/${jar}"
done

# 激活 Prometheus reporter（从 opt/ 复制到 lib/ 以启用）。
PROM_JAR="flink-metrics-prometheus-${FLINK_VERSION}.jar"
if [[ -f "${FLINK_HOME_LINK}/opt/${PROM_JAR}" && ! -f "${LIB_DIR}/${PROM_JAR}" ]]; then
    log_info "激活 Prometheus reporter（将 opt/${PROM_JAR} 复制到 lib/）"
    cp -f "${FLINK_HOME_LINK}/opt/${PROM_JAR}" "${LIB_DIR}/${PROM_JAR}"
fi

chown -R "${ORCP_USER}:${ORCP_GROUP}" "${FLINK_EXTRACT_DIR}"

# -------------------------------------------------------------------------
# 配置文件
# -------------------------------------------------------------------------
CONF_DIR="${FLINK_HOME_LINK}/conf"
JM_HOST="$(host_for 1)"
: "${JM_HOST:?未设置 NODE_1_HOST}"

log_info "安装 flink-conf.yaml"
# 替换模板中的占位符后写入配置。
FLINK_CONF_TEMPLATE="${SCRIPT_DIR}/../flink-conf/flink-conf.yaml"
sed "s#__JM_HOST__#${JM_HOST}#g; \
     s#__CHECKPOINT_DIR__#${CHECKPOINT_DIR}#g; \
     s#__SAVEPOINT_DIR__#${SAVEPOINT_DIR}#g" \
    "${FLINK_CONF_TEMPLATE}" >"${CONF_DIR}/flink-conf.yaml"

install -m 0644 "${SCRIPT_DIR}/../flink-conf/log4j.properties"    "${CONF_DIR}/log4j.properties"
install -m 0644 "${SCRIPT_DIR}/../flink-conf/log4j-cli.properties" "${CONF_DIR}/log4j-cli.properties" 2>/dev/null \
    || true
# metrics.yaml 仅用于文档说明；Prometheus reporter 配置已在 flink-conf.yaml 中。
install -m 0644 "${SCRIPT_DIR}/../flink-conf/metrics.yaml" "${CONF_DIR}/metrics.yaml"

log_info "渲染 masters + workers 文件"
printf '%s:8081\n' "${JM_HOST}" >"${CONF_DIR}/masters"
: >"${CONF_DIR}/workers"
if [[ "${NODE_COUNT}" -eq 1 ]]; then
    # 单节点 POC：JM 与 TM 同机运行。
    printf '%s\n' "${JM_HOST}" >>"${CONF_DIR}/workers"
else
    for i in $(seq 2 "${NODE_COUNT}"); do
        host="$(host_for "${i}")"
        [[ -n "${host}" ]] && printf '%s\n' "${host}" >>"${CONF_DIR}/workers"
    done
fi

chown -R "${ORCP_USER}:${ORCP_GROUP}" "${CONF_DIR}"

# -------------------------------------------------------------------------
# 日志目录软链接到 /var/log/orcp
# -------------------------------------------------------------------------
ensure_dir /var/log/orcp/flink "${ORCP_USER}:${ORCP_GROUP}" 0755
rm -rf "${FLINK_HOME_LINK}/log"
ln -s /var/log/orcp/flink "${FLINK_HOME_LINK}/log"

# -------------------------------------------------------------------------
# systemd 单元
# -------------------------------------------------------------------------
log_info "安装 systemd 单元（flink-jobmanager.service、flink-taskmanager.service）"
install_systemd_unit "${SCRIPT_DIR}/../systemd/flink-jobmanager.service"  flink-jobmanager.service
install_systemd_unit "${SCRIPT_DIR}/../systemd/flink-taskmanager.service" flink-taskmanager.service

if [[ "${NODE_ID}" == "1" ]]; then
    log_info "在 node-1 上启用 flink-jobmanager"
    systemctl enable --now flink-jobmanager.service
    if [[ "${NODE_COUNT}" -eq 1 ]]; then
        log_info "单节点 POC：同时启用 flink-taskmanager"
        systemctl enable --now flink-taskmanager.service
    fi
else
    log_info "在 node-${NODE_ID} 上启用 flink-taskmanager"
    systemctl enable --now flink-taskmanager.service
fi

log_info "等待 JobManager REST 端点就绪（最多 60 秒）"
for _ in $(seq 1 60); do
    if curl -fsS "http://${JM_HOST}:8081/overview" >/dev/null 2>&1; then
        log_info "JobManager 概览页面可访问：http://${JM_HOST}:8081"
        break
    fi
    sleep 1
done

log_info "30_install_flink.sh 执行完毕"
