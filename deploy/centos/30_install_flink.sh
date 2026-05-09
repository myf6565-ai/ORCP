#!/usr/bin/env bash
#
# 30_install_flink.sh --- Apache Flink 1.17.2 standalone cluster.
# See DEV_SPEC §5.5.
#
# Role per node:
#   NODE_ID == 1          -> JobManager (+ enable TaskManager only if single-node)
#   NODE_ID >= 2          -> TaskManager
#
# Connector jars are placed in /opt/flink/lib so the SQL client / fat jar can
# share them. The Prometheus reporter jar is moved from opt/ to lib/ per
# Flink docs.

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
# Download + layout
# -------------------------------------------------------------------------
if [[ ! -d "${FLINK_EXTRACT_DIR}" ]]; then
    log_info "Downloading Flink ${FLINK_VERSION}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${FLINK_TARBALL}"
    download_to "${FLINK_URL}" "${TMP_TAR}"
    tar -xzf "${TMP_TAR}" -C "${INSTALL_ROOT}"
fi

rm -f "${FLINK_HOME_LINK}"
ln -s "${FLINK_EXTRACT_DIR}" "${FLINK_HOME_LINK}"

ensure_dir "${CHECKPOINT_DIR}" "${ORCP_USER}:${ORCP_GROUP}" 0755
ensure_dir "${SAVEPOINT_DIR}"  "${ORCP_USER}:${ORCP_GROUP}" 0755

# -------------------------------------------------------------------------
# Connector jars in lib/
# -------------------------------------------------------------------------
LIB_DIR="${FLINK_HOME_LINK}/lib"
declare -A JARS
JARS["flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"]="https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"
JARS["flink-connector-jdbc-${FLINK_JDBC_CONNECTOR_VERSION}.jar"]="https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/${FLINK_JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${FLINK_JDBC_CONNECTOR_VERSION}.jar"
JARS["mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"]="https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar"
JARS["oceanbase-client-${OCEANBASE_CLIENT_VERSION}.jar"]="https://repo.maven.apache.org/maven2/com/oceanbase/oceanbase-client/${OCEANBASE_CLIENT_VERSION}/oceanbase-client-${OCEANBASE_CLIENT_VERSION}.jar"

for jar in "${!JARS[@]}"; do
    if [[ -f "${LIB_DIR}/${jar}" ]]; then
        log_info "Already present: ${jar}"
        continue
    fi
    log_info "Fetching ${jar}"
    download_to "${JARS[${jar}]}" "${LIB_DIR}/${jar}"
done

# Activate Prometheus reporter (ships in opt/; move to lib/ to enable).
PROM_JAR="flink-metrics-prometheus-${FLINK_VERSION}.jar"
if [[ -f "${FLINK_HOME_LINK}/opt/${PROM_JAR}" && ! -f "${LIB_DIR}/${PROM_JAR}" ]]; then
    log_info "Enabling Prometheus reporter (copying opt/${PROM_JAR} -> lib/)"
    cp -f "${FLINK_HOME_LINK}/opt/${PROM_JAR}" "${LIB_DIR}/${PROM_JAR}"
fi

chown -R "${ORCP_USER}:${ORCP_GROUP}" "${FLINK_EXTRACT_DIR}"

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------
CONF_DIR="${FLINK_HOME_LINK}/conf"
JM_HOST="$(host_for 1)"
: "${JM_HOST:?NODE_1_HOST not set}"

log_info "Installing flink-conf.yaml"
# Render from template, substituting only the JM host.
FLINK_CONF_TEMPLATE="${SCRIPT_DIR}/../flink-conf/flink-conf.yaml"
sed "s#__JM_HOST__#${JM_HOST}#g; \
     s#__CHECKPOINT_DIR__#${CHECKPOINT_DIR}#g; \
     s#__SAVEPOINT_DIR__#${SAVEPOINT_DIR}#g" \
    "${FLINK_CONF_TEMPLATE}" >"${CONF_DIR}/flink-conf.yaml"

install -m 0644 "${SCRIPT_DIR}/../flink-conf/log4j.properties"    "${CONF_DIR}/log4j.properties"
install -m 0644 "${SCRIPT_DIR}/../flink-conf/log4j-cli.properties" "${CONF_DIR}/log4j-cli.properties" 2>/dev/null \
    || true
# metrics.yaml is documentation; Prometheus reporter config already lives in flink-conf.yaml.
install -m 0644 "${SCRIPT_DIR}/../flink-conf/metrics.yaml" "${CONF_DIR}/metrics.yaml"

log_info "Rendering masters + workers"
printf '%s:8081\n' "${JM_HOST}" >"${CONF_DIR}/masters"
: >"${CONF_DIR}/workers"
if [[ "${NODE_COUNT}" -eq 1 ]]; then
    # Single-node POC: JM + TM on the same box.
    printf '%s\n' "${JM_HOST}" >>"${CONF_DIR}/workers"
else
    for i in $(seq 2 "${NODE_COUNT}"); do
        host="$(host_for "${i}")"
        [[ -n "${host}" ]] && printf '%s\n' "${host}" >>"${CONF_DIR}/workers"
    done
fi

chown -R "${ORCP_USER}:${ORCP_GROUP}" "${CONF_DIR}"

# -------------------------------------------------------------------------
# Logs -> /var/log/orcp
# -------------------------------------------------------------------------
ensure_dir /var/log/orcp/flink "${ORCP_USER}:${ORCP_GROUP}" 0755
rm -rf "${FLINK_HOME_LINK}/log"
ln -s /var/log/orcp/flink "${FLINK_HOME_LINK}/log"

# -------------------------------------------------------------------------
# systemd units
# -------------------------------------------------------------------------
log_info "Installing systemd units (flink-jobmanager.service, flink-taskmanager.service)"
install_systemd_unit "${SCRIPT_DIR}/../systemd/flink-jobmanager.service"  flink-jobmanager.service
install_systemd_unit "${SCRIPT_DIR}/../systemd/flink-taskmanager.service" flink-taskmanager.service

if [[ "${NODE_ID}" == "1" ]]; then
    log_info "Enabling flink-jobmanager on node-1"
    systemctl enable --now flink-jobmanager.service
    if [[ "${NODE_COUNT}" -eq 1 ]]; then
        log_info "Single-node POC: also enabling flink-taskmanager"
        systemctl enable --now flink-taskmanager.service
    fi
else
    log_info "Enabling flink-taskmanager on node-${NODE_ID}"
    systemctl enable --now flink-taskmanager.service
fi

log_info "Waiting up to 60s for the JobManager REST endpoint"
for _ in $(seq 1 60); do
    if curl -fsS "http://${JM_HOST}:8081/overview" >/dev/null 2>&1; then
        log_info "JobManager overview reachable at http://${JM_HOST}:8081"
        break
    fi
    sleep 1
done

log_info "30_install_flink.sh finished successfully"
