#!/usr/bin/env bash
#
# 15_install_zookeeper.sh --- Apache ZooKeeper 3.7.2，三节点集群。
# 参见 DEV_SPEC §5.3。
#
# 目录布局：
#   /opt/zookeeper               -> 版本化安装目录的符号链接
#   /data/zk/{data,log}          -> 数据目录 + 事务日志（建议使用快速磁盘）
#   /etc/systemd/system/zookeeper.service

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

: "${ZK_VERSION:=3.7.2}"
ZK_TARBALL="apache-zookeeper-${ZK_VERSION}-bin.tar.gz"
ZK_URL="${ORCP_APACHE_MIRROR}/zookeeper/zookeeper-${ZK_VERSION}/${ZK_TARBALL}"

INSTALL_ROOT=/opt
ZK_HOME_LINK="${INSTALL_ROOT}/zookeeper"
ZK_EXTRACT_DIR="${INSTALL_ROOT}/apache-zookeeper-${ZK_VERSION}-bin"
DATA_DIR=/data/zk/data
LOG_DIR=/data/zk/log

if [[ ! -d "${ZK_EXTRACT_DIR}" ]]; then
    log_info "下载 ZooKeeper ${ZK_VERSION}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${ZK_TARBALL}"
    download_to "${ZK_URL}" "${TMP_TAR}"
    tar -xzf "${TMP_TAR}" -C "${INSTALL_ROOT}"
fi

rm -f "${ZK_HOME_LINK}"
ln -s "${ZK_EXTRACT_DIR}" "${ZK_HOME_LINK}"
chown -R "${ORCP_USER}:${ORCP_GROUP}" "${ZK_EXTRACT_DIR}"

log_info "创建 ${DATA_DIR} 和 ${LOG_DIR}"
ensure_dir "${DATA_DIR}" "${ORCP_USER}:${ORCP_GROUP}" 0750
ensure_dir "${LOG_DIR}"  "${ORCP_USER}:${ORCP_GROUP}" 0750

log_info "写入 myid=${NODE_ID}"
printf '%s\n' "${NODE_ID}" >"${DATA_DIR}/myid"
chown "${ORCP_USER}:${ORCP_GROUP}" "${DATA_DIR}/myid"

log_info "渲染 conf/zoo.cfg"
ZK_CONF="${ZK_HOME_LINK}/conf/zoo.cfg"
{
    cat <<EOF
# 由 ORCP deploy/centos/15_install_zookeeper.sh 管理
tickTime=2000
initLimit=10
syncLimit=5
dataDir=${DATA_DIR}
dataLogDir=${LOG_DIR}
clientPort=2181
maxClientCnxns=200
autopurge.snapRetainCount=5
autopurge.purgeInterval=24
4lw.commands.whitelist=stat,ruok,conf,mntr
EOF
    for i in $(seq 1 "${NODE_COUNT}"); do
        host="$(host_for "${i}")"
        [[ -n "${host}" ]] && echo "server.${i}=${host}:2888:3888"
    done
} >"${ZK_CONF}"
chown "${ORCP_USER}:${ORCP_GROUP}" "${ZK_CONF}"

log_info "将 ZooKeeper 日志目录软链接到 /var/log/orcp"
ensure_dir /var/log/orcp/zookeeper "${ORCP_USER}:${ORCP_GROUP}" 0755
rm -f "${ZK_HOME_LINK}/logs"
ln -s /var/log/orcp/zookeeper "${ZK_HOME_LINK}/logs"

log_info "安装 systemd 单元 zookeeper.service"
install_systemd_unit "${SCRIPT_DIR}/../systemd/zookeeper.service" zookeeper.service

log_info "启用并启动 zookeeper.service"
systemctl enable --now zookeeper.service

log_info "等待 ZooKeeper 响应 ruok（最多 30 秒）"
for _ in $(seq 1 30); do
    if echo ruok | nc -w 1 127.0.0.1 2181 2>/dev/null | grep -q '^imok$'; then
        log_info "ZooKeeper 已响应 imok"
        exit 0
    fi
    sleep 1
done

log_warn "ZooKeeper 在 30 秒内未响应，请执行 'systemctl status zookeeper' 检查。"
exit 1
