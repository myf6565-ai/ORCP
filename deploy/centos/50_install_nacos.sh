#!/usr/bin/env bash
#
# 50_install_nacos.sh --- Nacos 2.2.3 standalone server (registry + config).
# See DEV_SPEC §5.8, §8.1 and §8.2. Runs on NODE_ID=1 only.
#
# For a true HA Nacos cluster you would point application.properties at an
# external MySQL and run it on all 3 nodes; that is intentionally out of scope
# for the minimum setup -- we ship standalone mode here.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

if [[ "${NODE_ID}" != "1" ]]; then
    log_info "Nacos runs on node-1 only (NODE_ID=${NODE_ID}); skipping."
    exit 0
fi

: "${NACOS_VERSION:=2.2.3}"
NACOS_TARBALL="nacos-server-${NACOS_VERSION}.tar.gz"
NACOS_URL="${ORCP_NACOS_URL:-https://github.com/alibaba/nacos/releases/download/${NACOS_VERSION}/${NACOS_TARBALL}}"

INSTALL_ROOT=/opt
NACOS_HOME="${INSTALL_ROOT}/nacos"
NACOS_DATA=/data/nacos

if [[ ! -d "${NACOS_HOME}" ]]; then
    log_info "Downloading Nacos ${NACOS_VERSION}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${NACOS_TARBALL}"
    download_to "${NACOS_URL}" "${TMP_TAR}"
    tar -xzf "${TMP_TAR}" -C "${INSTALL_ROOT}"
fi

chown -R "${ORCP_USER}:${ORCP_GROUP}" "${NACOS_HOME}"

ensure_dir "${NACOS_DATA}" "${ORCP_USER}:${ORCP_GROUP}" 0750

log_info "Writing minimal application.properties (standalone, embedded storage)"
cat >"${NACOS_HOME}/conf/application.properties" <<EOF
# Managed by ORCP deploy/centos/50_install_nacos.sh
server.servlet.contextPath=/nacos
server.port=8848
nacos.inetutils.ip-address=$(host_for "${NODE_ID}")
nacos.core.auth.enabled=false
nacos.core.auth.system.type=nacos

# Embedded derby storage suitable for the minimum deployment; swap to external
# MySQL when you need HA.
spring.datasource.platform=
nacos.home=${NACOS_HOME}
nacos.logs.path=/var/log/orcp/nacos
EOF
chown "${ORCP_USER}:${ORCP_GROUP}" "${NACOS_HOME}/conf/application.properties"

ensure_dir /var/log/orcp/nacos "${ORCP_USER}:${ORCP_GROUP}" 0755
rm -rf "${NACOS_HOME}/logs"
ln -s /var/log/orcp/nacos "${NACOS_HOME}/logs"

log_info "Installing systemd unit nacos.service"
install_systemd_unit "${SCRIPT_DIR}/../systemd/nacos.service" nacos.service

log_info "Enabling + starting nacos.service"
systemctl enable --now nacos.service

log_info "Waiting up to 60s for Nacos health endpoint"
for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:8848/nacos/v1/console/health/readiness" >/dev/null 2>&1; then
        log_info "Nacos is up; UI at http://$(host_for "${NODE_ID}"):8848/nacos (nacos/nacos)"
        exit 0
    fi
    sleep 1
done

log_warn "Nacos did not become ready within 60s; check 'systemctl status nacos'."
exit 1
