#!/usr/bin/env bash
#
# 00_bootstrap.sh --- 所有 ORCP 节点的操作系统级初始化。
#
# 职责（参见 DEV_SPEC §5.1）：
#   - 创建 orcp 用户/组
#   - 关闭 SELinux 和 firewalld（内网假设）
#   - 配置 Asia/Shanghai 时区 + chrony NTP
#   - 将 node-1/node-2/node-3 写入 /etc/hosts
#   - 提升 orcp 用户的 nofile/nproc 资源限制
#   - 关闭 THP 和 swap（Kafka/Flink 推荐）
#   - 安装常用运行时依赖（curl、tar、jq、nc、rsync）
#
# 幂等性：可安全重复运行。

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

log_info "安装基础软件包"
if command -v dnf >/dev/null 2>&1; then
    dnf -y install curl tar gzip jq nmap-ncat rsync chrony procps-ng util-linux
else
    yum -y install curl tar gzip jq nmap-ncat rsync chrony procps-ng util-linux
fi

log_info "创建 ${ORCP_GROUP}/${ORCP_USER}（如不存在）"
getent group "${ORCP_GROUP}" >/dev/null || groupadd --system "${ORCP_GROUP}"
if ! getent passwd "${ORCP_USER}" >/dev/null; then
    useradd --system --create-home --home-dir "/home/${ORCP_USER}" \
            --shell /bin/bash --gid "${ORCP_GROUP}" "${ORCP_USER}"
fi

log_info "为 ${ORCP_USER} 配置免密 sudo"
cat >/etc/sudoers.d/orcp <<EOF
${ORCP_USER} ALL=(ALL) NOPASSWD:ALL
EOF
chmod 0440 /etc/sudoers.d/orcp

log_info "关闭 SELinux"
if command -v setenforce >/dev/null 2>&1; then
    setenforce 0 || true
fi
if [[ -f /etc/selinux/config ]]; then
    sed -ri 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
fi

log_info "关闭 firewalld（内网环境）"
systemctl disable --now firewalld 2>/dev/null || true

log_info "设置时区为 Asia/Shanghai 并启动 chronyd"
timedatectl set-timezone Asia/Shanghai || true
systemctl enable --now chronyd

log_info "将集群主机名写入 /etc/hosts"
# 幂等性：先删除旧的 ORCP 块，再追加新条目。
sed -i '/# BEGIN ORCP-HOSTS/,/# END ORCP-HOSTS/d' /etc/hosts
{
    echo "# BEGIN ORCP-HOSTS"
    for i in $(seq 1 "${NODE_COUNT}"); do
        host="$(host_for "${i}")"
        if [[ -n "${host}" ]]; then
            # 尝试 DNS 解析；失败则留作占位，运维后续手动补充真实 IP。
            ip="$(getent ahosts "${host}" | awk 'NR==1{print $1}' || true)"
            if [[ -z "${ip}" ]]; then
                log_warn "无法解析 ${host}，/etc/hosts 中使用占位 IP，请后续手动修正。"
                ip="127.0.0.1"
            fi
            printf '%s %s\n' "${ip}" "${host}"
        fi
    done
    echo "# END ORCP-HOSTS"
} >>/etc/hosts

log_info "提升 ${ORCP_USER} 的资源限制"
cat >/etc/security/limits.d/99-orcp.conf <<EOF
${ORCP_USER} soft nofile 655360
${ORCP_USER} hard nofile 655360
${ORCP_USER} soft nproc  655360
${ORCP_USER} hard nproc  655360
EOF

log_info "在启动时和运行时关闭透明大页（THP）"
cat >/etc/systemd/system/disable-thp.service <<'EOF'
[Unit]
Description=禁用透明大页（THP），为 Kafka/Flink 优化内存性能
DefaultDependencies=no
After=sysinit.target local-fs.target
Before=basic.target

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/enabled && echo never > /sys/kernel/mm/transparent_hugepage/defrag'
RemainAfterExit=yes

[Install]
WantedBy=basic.target
EOF
systemctl daemon-reload
systemctl enable --now disable-thp.service

log_info "关闭 swap"
swapoff -a || true
sed -ri 's@^([^#].*\sswap\s.*)$@# \1@' /etc/fstab

log_info "创建 /var/log/orcp 和 /data 目录"
install -d -m 0755 -o "${ORCP_USER}" -g "${ORCP_GROUP}" /var/log/orcp
install -d -m 0755 /data

log_info "00_bootstrap.sh 在 $(hostname) 上执行完毕"
