#!/usr/bin/env bash
# deploy/centos/*.sh 的共享辅助函数。使用方式：
#
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   # shellcheck disable=SC1091
#   source "${SCRIPT_DIR}/_lib.sh"
#
# 提供以下能力：
#   - log_info / log_warn / log_err    结构化 stderr 日志
#   - require_root                     非 root 时退出
#   - load_cluster_env                 加载 /etc/orcp/cluster.env
#   - download_to <url> <dest>         带缓存的 curl 下载
#   - install_systemd_unit <src> <name>  复制单元文件并执行 systemctl daemon-reload
#   - host_for <id>                    输出 NODE_<id>_HOST

set -euo pipefail

# ---------------------------------------------------------------------------
# 日志（写入 stderr，stdout 保留给脚本输出）
# ---------------------------------------------------------------------------
_log() {
    local level="$1"; shift
    printf '[%s] [%s] %s\n' "$(date -Iseconds)" "${level}" "$*" >&2
}
log_info() { _log INFO  "$@"; }
log_warn() { _log WARN  "$@"; }
log_err()  { _log ERROR "$@"; }

# ---------------------------------------------------------------------------
# 前置检查
# ---------------------------------------------------------------------------
require_root() {
    if [[ "$(id -u)" -ne 0 ]]; then
        log_err "本脚本必须以 root 身份运行（请使用 sudo）。"
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# 集群环境变量
# ---------------------------------------------------------------------------
: "${ORCP_CLUSTER_ENV:=/etc/orcp/cluster.env}"

load_cluster_env() {
    if [[ ! -f "${ORCP_CLUSTER_ENV}" ]]; then
        log_err "找不到 ${ORCP_CLUSTER_ENV}，请先将 deploy/centos/cluster.env.example 复制到该路径。"
        exit 1
    fi
    # shellcheck disable=SC1090
    source "${ORCP_CLUSTER_ENV}"

    : "${NODE_ID:?${ORCP_CLUSTER_ENV} 中未设置 NODE_ID}"
    : "${NODE_COUNT:?${ORCP_CLUSTER_ENV} 中未设置 NODE_COUNT}"
    : "${NODE_1_HOST:?未设置 NODE_1_HOST}"
    : "${ORCP_USER:=orcp}"
    : "${ORCP_GROUP:=orcp}"
    : "${ORCP_APACHE_MIRROR:=https://archive.apache.org/dist}"
    : "${ORCP_DOWNLOAD_CACHE:=/var/cache/orcp/downloads}"

    mkdir -p "${ORCP_DOWNLOAD_CACHE}"
}

host_for() {
    local id="$1"
    local var="NODE_${id}_HOST"
    printf '%s' "${!var:-}"
}

# ---------------------------------------------------------------------------
# 下载（使用 ${ORCP_DOWNLOAD_CACHE} 作为按文件名缓存的本地镜像）
# 用法：download_to <url> <目标路径>
# ---------------------------------------------------------------------------
download_to() {
    local url="$1"
    local dest="$2"
    local cache_name
    cache_name="$(basename "${dest}")"
    local cache_path="${ORCP_DOWNLOAD_CACHE}/${cache_name}"

    if [[ -f "${cache_path}" ]]; then
        log_info "使用本地缓存：${cache_path}"
    else
        log_info "下载：${url}"
        curl -fLsS --retry 5 --retry-delay 5 -o "${cache_path}.part" "${url}"
        mv "${cache_path}.part" "${cache_path}"
    fi
    cp -f "${cache_path}" "${dest}"
}

# ---------------------------------------------------------------------------
# systemd 辅助函数
# ---------------------------------------------------------------------------
install_systemd_unit() {
    local src="$1"
    local name="$2"
    install -m 0644 -o root -g root "${src}" "/etc/systemd/system/${name}"
    systemctl daemon-reload
    log_info "已安装 systemd 单元：${name}"
}

ensure_dir() {
    local path="$1"
    local owner="${2:-${ORCP_USER}:${ORCP_GROUP}}"
    local mode="${3:-0755}"
    install -d -m "${mode}" -o "${owner%:*}" -g "${owner#*:}" "${path}"
}
