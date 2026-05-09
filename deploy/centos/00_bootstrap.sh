#!/usr/bin/env bash
#
# 00_bootstrap.sh --- OS-level preparation for every ORCP node.
#
# Responsibilities (see DEV_SPEC §5.1):
#   - Create the orcp user/group
#   - Disable SELinux and firewalld (internal network assumption)
#   - Configure Asia/Shanghai timezone + chrony NTP
#   - Populate /etc/hosts with node-1/node-2/node-3
#   - Raise nofile/nproc limits for the orcp user
#   - Disable THP and swap (Kafka/Flink recommendation)
#   - Install common runtime deps (curl, tar, jq, nc, rsync)
#
# Idempotent: safe to re-run.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

log_info "Installing base packages"
if command -v dnf >/dev/null 2>&1; then
    dnf -y install curl tar gzip jq nmap-ncat rsync chrony procps-ng util-linux
else
    yum -y install curl tar gzip jq nmap-ncat rsync chrony procps-ng util-linux
fi

log_info "Creating ${ORCP_GROUP}/${ORCP_USER} (if missing)"
getent group "${ORCP_GROUP}" >/dev/null || groupadd --system "${ORCP_GROUP}"
if ! getent passwd "${ORCP_USER}" >/dev/null; then
    useradd --system --create-home --home-dir "/home/${ORCP_USER}" \
            --shell /bin/bash --gid "${ORCP_GROUP}" "${ORCP_USER}"
fi

log_info "Granting passwordless sudo to ${ORCP_USER}"
cat >/etc/sudoers.d/orcp <<EOF
${ORCP_USER} ALL=(ALL) NOPASSWD:ALL
EOF
chmod 0440 /etc/sudoers.d/orcp

log_info "Disabling SELinux"
if command -v setenforce >/dev/null 2>&1; then
    setenforce 0 || true
fi
if [[ -f /etc/selinux/config ]]; then
    sed -ri 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
fi

log_info "Disabling firewalld (internal network assumption)"
systemctl disable --now firewalld 2>/dev/null || true

log_info "Setting timezone to Asia/Shanghai and enabling chronyd"
timedatectl set-timezone Asia/Shanghai || true
systemctl enable --now chronyd

log_info "Updating /etc/hosts with cluster topology"
# Idempotent: remove prior ORCP block and append fresh entries.
sed -i '/# BEGIN ORCP-HOSTS/,/# END ORCP-HOSTS/d' /etc/hosts
{
    echo "# BEGIN ORCP-HOSTS"
    for i in $(seq 1 "${NODE_COUNT}"); do
        host="$(host_for "${i}")"
        if [[ -n "${host}" ]]; then
            # Try to resolve via DNS; if it fails, operator must set real IPs later.
            ip="$(getent ahosts "${host}" | awk 'NR==1{print $1}' || true)"
            if [[ -z "${ip}" ]]; then
                log_warn "Could not resolve ${host}; leaving /etc/hosts entry as placeholder."
                ip="127.0.0.1"
            fi
            printf '%s %s\n' "${ip}" "${host}"
        fi
    done
    echo "# END ORCP-HOSTS"
} >>/etc/hosts

log_info "Raising ulimits for ${ORCP_USER}"
cat >/etc/security/limits.d/99-orcp.conf <<EOF
${ORCP_USER} soft nofile 655360
${ORCP_USER} hard nofile 655360
${ORCP_USER} soft nproc  655360
${ORCP_USER} hard nproc  655360
EOF

log_info "Disabling Transparent Huge Pages at boot and for the running kernel"
cat >/etc/systemd/system/disable-thp.service <<'EOF'
[Unit]
Description=Disable Transparent Huge Pages (THP) for Kafka/Flink
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

log_info "Disabling swap"
swapoff -a || true
sed -ri 's@^([^#].*\sswap\s.*)$@# \1@' /etc/fstab

log_info "Creating /var/log/orcp and /data"
install -d -m 0755 -o "${ORCP_USER}" -g "${ORCP_GROUP}" /var/log/orcp
install -d -m 0755 /data

log_info "00_bootstrap.sh finished successfully on $(hostname)"
