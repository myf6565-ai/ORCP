#!/usr/bin/env bash
#
# 20_install_kafka_zk.sh --- Apache Kafka 3.5.2 in ZooKeeper mode.
# See DEV_SPEC §5.4.
#
# Layout:
#   /opt/kafka                -> versioned install symlink
#   /data/kafka-logs          -> log.dirs (message segments)
#   /etc/systemd/system/kafka.service
#
# Assumes ZooKeeper is already running across the ensemble (15_install_zookeeper.sh).

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

: "${KAFKA_VERSION:=3.5.2}"
: "${KAFKA_SCALA:=2.13}"
KAFKA_TARBALL="kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
KAFKA_URL="${ORCP_APACHE_MIRROR}/kafka/${KAFKA_VERSION}/${KAFKA_TARBALL}"

INSTALL_ROOT=/opt
KAFKA_HOME_LINK="${INSTALL_ROOT}/kafka"
KAFKA_EXTRACT_DIR="${INSTALL_ROOT}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}"
DATA_DIR=/data/kafka-logs

if [[ ! -d "${KAFKA_EXTRACT_DIR}" ]]; then
    log_info "Downloading Kafka ${KAFKA_VERSION}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${KAFKA_TARBALL}"
    download_to "${KAFKA_URL}" "${TMP_TAR}"
    tar -xzf "${TMP_TAR}" -C "${INSTALL_ROOT}"
fi

rm -f "${KAFKA_HOME_LINK}"
ln -s "${KAFKA_EXTRACT_DIR}" "${KAFKA_HOME_LINK}"
chown -R "${ORCP_USER}:${ORCP_GROUP}" "${KAFKA_EXTRACT_DIR}"

ensure_dir "${DATA_DIR}" "${ORCP_USER}:${ORCP_GROUP}" 0750

# Build the ZooKeeper connect string (all nodes, under chroot /kafka).
ZK_CONNECT=""
for i in $(seq 1 "${NODE_COUNT}"); do
    host="$(host_for "${i}")"
    [[ -z "${host}" ]] && continue
    ZK_CONNECT="${ZK_CONNECT:+${ZK_CONNECT},}${host}:2181"
done
ZK_CONNECT="${ZK_CONNECT}/kafka"

THIS_HOST="$(host_for "${NODE_ID}")"
: "${THIS_HOST:?Cannot resolve NODE_${NODE_ID}_HOST}"

# Replication factor + ISR track the cluster size but clamp at 3 and 2 for
# larger clusters (and degrade sensibly for single-node POCs).
REP_FACTOR=$(( NODE_COUNT < 3 ? NODE_COUNT : 3 ))
MIN_ISR=$(( REP_FACTOR < 2 ? 1 : 2 ))

log_info "Rendering config/server.properties for broker.id=${NODE_ID}"
KAFKA_CONF="${KAFKA_HOME_LINK}/config/server.properties"
cat >"${KAFKA_CONF}" <<EOF
# Managed by ORCP deploy/centos/20_install_kafka_zk.sh
broker.id=${NODE_ID}

listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://${THIS_HOST}:9092
inter.broker.listener.name=PLAINTEXT

num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

log.dirs=${DATA_DIR}
num.partitions=3
num.recovery.threads.per.data.dir=1
default.replication.factor=${REP_FACTOR}
min.insync.replicas=${MIN_ISR}

offsets.topic.replication.factor=${REP_FACTOR}
transaction.state.log.replication.factor=${REP_FACTOR}
transaction.state.log.min.isr=${MIN_ISR}

log.retention.hours=72
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

zookeeper.connect=${ZK_CONNECT}
zookeeper.connection.timeout.ms=18000

group.initial.rebalance.delay.ms=3000
auto.create.topics.enable=false
unclean.leader.election.enable=false
EOF
chown "${ORCP_USER}:${ORCP_GROUP}" "${KAFKA_CONF}"

log_info "Linking Kafka logs into /var/log/orcp"
ensure_dir /var/log/orcp/kafka "${ORCP_USER}:${ORCP_GROUP}" 0755
rm -rf "${KAFKA_HOME_LINK}/logs"
ln -s /var/log/orcp/kafka "${KAFKA_HOME_LINK}/logs"

log_info "Installing systemd unit kafka.service"
install_systemd_unit "${SCRIPT_DIR}/../systemd/kafka.service" kafka.service

log_info "Enabling + starting kafka.service"
systemctl enable --now kafka.service

log_info "Waiting up to 45s for Kafka to accept connections on :9092"
for _ in $(seq 1 45); do
    if (echo > "/dev/tcp/127.0.0.1/9092") 2>/dev/null; then
        log_info "Kafka is listening"
        break
    fi
    sleep 1
done

# Create the canonical topics only from the first node (idempotent check).
if [[ "${NODE_ID}" == "1" ]]; then
    log_info "Ensuring topics orcp.src.demo / orcp.mid.events exist"
    for topic in orcp.src.demo orcp.mid.events; do
        if ! "${KAFKA_HOME_LINK}/bin/kafka-topics.sh" \
                --bootstrap-server "${THIS_HOST}:9092" \
                --list 2>/dev/null | grep -qx "${topic}"; then
            "${KAFKA_HOME_LINK}/bin/kafka-topics.sh" \
                --bootstrap-server "${THIS_HOST}:9092" \
                --create --topic "${topic}" \
                --partitions 3 --replication-factor "${REP_FACTOR}"
            log_info "Created topic ${topic}"
        else
            log_info "Topic ${topic} already exists"
        fi
    done
fi

log_info "20_install_kafka_zk.sh finished successfully"
