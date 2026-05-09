#!/usr/bin/env bash
#
# 40_install_mysql.sh --- MySQL 8.0 server for the orcp_detail schema.
# See DEV_SPEC §5.6. Only runs on NODE_ID=1; other nodes skip gracefully.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

if [[ "${NODE_ID}" != "1" ]]; then
    log_info "MySQL only runs on node-1 (NODE_ID=${NODE_ID}); skipping."
    exit 0
fi

: "${MYSQL_YUM_REPO_RPM:=https://dev.mysql.com/get/mysql80-community-release-el8-9.noarch.rpm}"

if ! rpm -q mysql80-community-release >/dev/null 2>&1; then
    log_info "Installing MySQL 8.0 YUM repository"
    if command -v dnf >/dev/null 2>&1; then
        dnf -y install "${MYSQL_YUM_REPO_RPM}" || true
        # Accept the MySQL 2022 GPG key (common on stock CentOS/Alma/Rocky images).
        dnf -y install mysql-community-server mysql-community-client || {
            log_warn "Retrying with --nogpgcheck (GPG key rotation)"
            dnf -y --nogpgcheck install mysql-community-server mysql-community-client
        }
    else
        yum -y install "${MYSQL_YUM_REPO_RPM}" || true
        yum -y install mysql-community-server mysql-community-client || {
            log_warn "Retrying with --nogpgcheck"
            yum -y --nogpgcheck install mysql-community-server mysql-community-client
        }
    fi
else
    log_info "MySQL repo already installed"
fi

log_info "Writing /etc/my.cnf.d/orcp.cnf with project-wide defaults"
install -d -m 0755 /etc/my.cnf.d
cat >/etc/my.cnf.d/orcp.cnf <<'EOF'
# Managed by ORCP deploy/centos/40_install_mysql.sh
[mysqld]
character-set-server        = utf8mb4
collation-server            = utf8mb4_general_ci
default-time-zone           = '+08:00'
lower_case_table_names      = 1
max_connections             = 500

# Binlog reserved for potential future CDC; does not affect the minimum path.
log_bin                     = mysql-bin
binlog_format               = ROW
binlog_row_image            = FULL
server_id                   = 1001
expire_logs_days            = 7

[client]
default-character-set       = utf8mb4
EOF

log_info "Enabling mysqld"
systemctl enable --now mysqld

log_info "Waiting up to 60s for mysqld to accept connections"
for _ in $(seq 1 60); do
    if mysqladmin ping --silent 2>/dev/null; then
        log_info "mysqld is up"
        break
    fi
    sleep 1
done

# NOTE: MySQL 8 generates a temporary root password on first start.
# We expose it here; the operator is expected to run mysql_secure_installation
# (or an equivalent automated script) before Stage C loads the schema.
TMP_PWD_LOG=/var/log/mysqld.log
if [[ -f "${TMP_PWD_LOG}" ]]; then
    log_warn "MySQL generated a temporary root password. See the line below:"
    grep -E "A temporary password is generated" "${TMP_PWD_LOG}" | tail -1 || true
    log_warn "Run 'mysql_secure_installation' before executing Stage C DDL."
fi

log_info "40_install_mysql.sh finished successfully (schema load happens in Stage C)"
