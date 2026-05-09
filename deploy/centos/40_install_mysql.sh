#!/usr/bin/env bash
#
# 40_install_mysql.sh --- MySQL 8.0 服务端（用于 orcp_detail 明细库）。
# 参见 DEV_SPEC §5.6。仅在 NODE_ID=1 时运行；其他节点会优雅跳过。

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

if [[ "${NODE_ID}" != "1" ]]; then
    log_info "MySQL 仅在 node-1 运行（当前 NODE_ID=${NODE_ID}），跳过。"
    exit 0
fi

: "${MYSQL_YUM_REPO_RPM:=https://dev.mysql.com/get/mysql80-community-release-el8-9.noarch.rpm}"

if ! rpm -q mysql80-community-release >/dev/null 2>&1; then
    log_info "安装 MySQL 8.0 YUM 仓库"
    if command -v dnf >/dev/null 2>&1; then
        dnf -y install "${MYSQL_YUM_REPO_RPM}" || true
        # 接受 MySQL 2022 GPG 密钥（CentOS/Alma/Rocky 常见问题）。
        dnf -y install mysql-community-server mysql-community-client || {
            log_warn "重试（忽略 GPG 检查，因密钥轮换）"
            dnf -y --nogpgcheck install mysql-community-server mysql-community-client
        }
    else
        yum -y install "${MYSQL_YUM_REPO_RPM}" || true
        yum -y install mysql-community-server mysql-community-client || {
            log_warn "重试（忽略 GPG 检查）"
            yum -y --nogpgcheck install mysql-community-server mysql-community-client
        }
    fi
else
    log_info "MySQL 仓库已安装"
fi

log_info "写入项目默认配置 /etc/my.cnf.d/orcp.cnf"
install -d -m 0755 /etc/my.cnf.d
cat >/etc/my.cnf.d/orcp.cnf <<'EOF'
# 由 ORCP deploy/centos/40_install_mysql.sh 管理
[mysqld]
character-set-server        = utf8mb4
collation-server            = utf8mb4_general_ci
default-time-zone           = '+08:00'
lower_case_table_names      = 1
max_connections             = 500

# binlog 为将来可选 CDC 预留，不属于当前最小方案的核心路径。
log_bin                     = mysql-bin
binlog_format               = ROW
binlog_row_image            = FULL
server_id                   = 1001
expire_logs_days            = 7

[client]
default-character-set       = utf8mb4
EOF

log_info "启用 mysqld"
systemctl enable --now mysqld

log_info "等待 mysqld 接受连接（最多 60 秒）"
for _ in $(seq 1 60); do
    if mysqladmin ping --silent 2>/dev/null; then
        log_info "mysqld 已就绪"
        break
    fi
    sleep 1
done

# MySQL 8 首次启动时会生成临时 root 密码，建议运维人员执行 mysql_secure_installation。
TMP_PWD_LOG=/var/log/mysqld.log
if [[ -f "${TMP_PWD_LOG}" ]]; then
    log_warn "MySQL 已生成临时 root 密码，详见下方日志行："
    grep -E "A temporary password is generated" "${TMP_PWD_LOG}" | tail -1 || true
    log_warn "请在执行阶段 C 建表之前运行 'mysql_secure_installation'。"
fi

log_info "40_install_mysql.sh 执行完毕（表结构加载在阶段 C 完成）"
