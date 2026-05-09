#!/usr/bin/env bash
#
# 50_install_nacos.sh --- Nacos 2.2.3 standalone 服务端（注册中心 + 配置中心）。
# 参见 DEV_SPEC §5.8、§8.1 和 §8.2。仅在 NODE_ID=1 时运行。
#
# 如需 HA Nacos 集群，应将 application.properties 指向外部 MySQL 并在全部三个节点部署；
# 该场景超出最小方案范围，此处仅提供 standalone 模式。

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

if [[ "${NODE_ID}" != "1" ]]; then
    log_info "Nacos 仅在 node-1 运行（当前 NODE_ID=${NODE_ID}），跳过。"
    exit 0
fi

: "${NACOS_VERSION:=2.2.3}"
NACOS_TARBALL="nacos-server-${NACOS_VERSION}.tar.gz"
NACOS_URL="${ORCP_NACOS_URL:-https://github.com/alibaba/nacos/releases/download/${NACOS_VERSION}/${NACOS_TARBALL}}"

INSTALL_ROOT=/opt
NACOS_HOME="${INSTALL_ROOT}/nacos"
NACOS_DATA=/data/nacos

if [[ ! -d "${NACOS_HOME}" ]]; then
    log_info "下载 Nacos ${NACOS_VERSION}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${NACOS_TARBALL}"
    download_to "${NACOS_URL}" "${TMP_TAR}"
    tar -xzf "${TMP_TAR}" -C "${INSTALL_ROOT}"
fi

chown -R "${ORCP_USER}:${ORCP_GROUP}" "${NACOS_HOME}"

ensure_dir "${NACOS_DATA}" "${ORCP_USER}:${ORCP_GROUP}" 0750

log_info "写入最小化 application.properties（standalone 模式，内嵌存储）"
cat >"${NACOS_HOME}/conf/application.properties" <<EOF
# 由 ORCP deploy/centos/50_install_nacos.sh 管理
server.servlet.contextPath=/nacos
server.port=8848
nacos.inetutils.ip-address=$(host_for "${NODE_ID}")
nacos.core.auth.enabled=false
nacos.core.auth.system.type=nacos

# 内嵌 derby 存储适合最小化部署；如需 HA 请切换到外部 MySQL。
spring.datasource.platform=
nacos.home=${NACOS_HOME}
nacos.logs.path=/var/log/orcp/nacos
EOF
chown "${ORCP_USER}:${ORCP_GROUP}" "${NACOS_HOME}/conf/application.properties"

ensure_dir /var/log/orcp/nacos "${ORCP_USER}:${ORCP_GROUP}" 0755
rm -rf "${NACOS_HOME}/logs"
ln -s /var/log/orcp/nacos "${NACOS_HOME}/logs"

log_info "安装 systemd 单元 nacos.service"
install_systemd_unit "${SCRIPT_DIR}/../systemd/nacos.service" nacos.service

log_info "启用并启动 nacos.service"
systemctl enable --now nacos.service

log_info "等待 Nacos 健康检查端点就绪（最多 60 秒）"
for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:8848/nacos/v1/console/health/readiness" >/dev/null 2>&1; then
        log_info "Nacos 已就绪；UI 地址：http://$(host_for "${NODE_ID}"):8848/nacos（默认 nacos/nacos）"
        exit 0
    fi
    sleep 1
done

log_warn "Nacos 在 60 秒内未就绪，请检查 'systemctl status nacos'。"
exit 1
