#!/usr/bin/env bash
#
# 60_install_prom_grafana.sh --- Prometheus 2.45.x + Grafana 10.1.x。
# 参见 DEV_SPEC §8.4。默认在 NODE_ID=2（监控节点）运行，可通过变量覆盖。

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

: "${ORCP_MONITORING_NODE_ID:=2}"
if [[ "${NODE_ID}" != "${ORCP_MONITORING_NODE_ID}" ]]; then
    log_info "监控栈仅在 node-${ORCP_MONITORING_NODE_ID} 运行（当前 NODE_ID=${NODE_ID}），跳过。"
    exit 0
fi

# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------
: "${PROM_VERSION:=2.45.6}"
PROM_TARBALL="prometheus-${PROM_VERSION}.linux-amd64.tar.gz"
PROM_URL="https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/${PROM_TARBALL}"
PROM_EXTRACT="/opt/prometheus-${PROM_VERSION}.linux-amd64"
PROM_HOME_LINK=/opt/prometheus
PROM_DATA=/data/prometheus

if [[ ! -d "${PROM_EXTRACT}" ]]; then
    log_info "下载 Prometheus ${PROM_VERSION}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${PROM_TARBALL}"
    download_to "${PROM_URL}" "${TMP_TAR}"
    tar -xzf "${TMP_TAR}" -C /opt
fi
rm -f "${PROM_HOME_LINK}"
ln -s "${PROM_EXTRACT}" "${PROM_HOME_LINK}"
ensure_dir "${PROM_DATA}" "${ORCP_USER}:${ORCP_GROUP}" 0750
chown -R "${ORCP_USER}:${ORCP_GROUP}" "${PROM_EXTRACT}"

log_info "渲染 Prometheus 抓取配置"
PROM_CONF="${PROM_HOME_LINK}/prometheus.yml"
{
    cat <<'EOF'
# 由 ORCP deploy/centos/60_install_prom_grafana.sh 管理
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['127.0.0.1:9090']

  - job_name: flink
    metrics_path: /
    static_configs:
      - targets:
EOF
    # Flink PrometheusReporter 使用端口范围 9250-9260（参见 flink-conf.yaml）。
    for i in $(seq 1 "${NODE_COUNT}"); do
        host="$(host_for "${i}")"
        [[ -z "${host}" ]] && continue
        for port in 9250 9251 9252 9253; do
            echo "        - '${host}:${port}'"
        done
    done
    cat <<'EOF'

  - job_name: orcp-spring
    metrics_path: /actuator/prometheus
    static_configs:
      - targets:
EOF
    # orcp-ingest 在 NODE_3 的 8080 端口，orcp-admin 在 NODE_3 的 8081 端口（参见 DEV_SPEC §3）。
    ingest_host="$(host_for 3)"
    admin_host="$(host_for 3)"
    [[ -n "${ingest_host}" ]] && echo "        - '${ingest_host}:8080'"
    [[ -n "${admin_host}"  ]] && echo "        - '${admin_host}:8081'"
} >"${PROM_CONF}"
chown "${ORCP_USER}:${ORCP_GROUP}" "${PROM_CONF}"

log_info "安装 systemd 单元 prometheus.service"
cat >/etc/systemd/system/prometheus.service <<EOF
[Unit]
Description=Prometheus（ORCP 监控）
After=network-online.target

[Service]
Type=simple
User=${ORCP_USER}
Group=${ORCP_GROUP}
ExecStart=${PROM_HOME_LINK}/prometheus \\
    --config.file=${PROM_HOME_LINK}/prometheus.yml \\
    --storage.tsdb.path=${PROM_DATA} \\
    --web.listen-address=0.0.0.0:9090
Restart=on-failure
RestartSec=10
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now prometheus.service

# ---------------------------------------------------------------------------
# Grafana
# ---------------------------------------------------------------------------
log_info "安装 Grafana OSS"
cat >/etc/yum.repos.d/grafana.repo <<'EOF'
[grafana]
name=grafana
baseurl=https://rpm.grafana.com
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://rpm.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
EOF

if command -v dnf >/dev/null 2>&1; then
    dnf -y install grafana || dnf -y --nogpgcheck install grafana
else
    yum -y install grafana || yum -y --nogpgcheck install grafana
fi

log_info "启用 grafana-server（默认端口 3000，首次登录 admin/admin，请立即修改）"
systemctl enable --now grafana-server

log_info "60_install_prom_grafana.sh 执行完毕"
log_info "Prometheus: http://$(host_for "${NODE_ID}"):9090"
log_info "Grafana:    http://$(host_for "${NODE_ID}"):3000（admin/admin —— 首次登录后请修改密码）"
