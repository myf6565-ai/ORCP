#!/usr/bin/env bash
# Shared helpers for deploy/centos/*.sh. Source with:
#
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   # shellcheck disable=SC1091
#   source "${SCRIPT_DIR}/_lib.sh"
#
# Provides:
#   - log_info / log_warn / log_err    structured stderr logging
#   - require_root                     exits unless running under sudo/root
#   - load_cluster_env                 loads /etc/orcp/cluster.env
#   - download_to <url> <dest>         cached, checksummed download (curl)
#   - install_systemd_unit <src> <name>  copies unit + systemctl daemon-reload
#   - host_for <id>                    echoes NODE_<id>_HOST

set -euo pipefail

# ---------------------------------------------------------------------------
# Logging (stderr, stdout is reserved for script output)
# ---------------------------------------------------------------------------
_log() {
    local level="$1"; shift
    printf '[%s] [%s] %s\n' "$(date -Iseconds)" "${level}" "$*" >&2
}
log_info() { _log INFO  "$@"; }
log_warn() { _log WARN  "$@"; }
log_err()  { _log ERROR "$@"; }

# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------
require_root() {
    if [[ "$(id -u)" -ne 0 ]]; then
        log_err "This script must run as root (use sudo)."
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Cluster env
# ---------------------------------------------------------------------------
: "${ORCP_CLUSTER_ENV:=/etc/orcp/cluster.env}"

load_cluster_env() {
    if [[ ! -f "${ORCP_CLUSTER_ENV}" ]]; then
        log_err "Missing ${ORCP_CLUSTER_ENV}. Copy deploy/centos/cluster.env.example there first."
        exit 1
    fi
    # shellcheck disable=SC1090
    source "${ORCP_CLUSTER_ENV}"

    : "${NODE_ID:?NODE_ID not set in ${ORCP_CLUSTER_ENV}}"
    : "${NODE_COUNT:?NODE_COUNT not set in ${ORCP_CLUSTER_ENV}}"
    : "${NODE_1_HOST:?NODE_1_HOST not set}"
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
# Downloads. Uses ${ORCP_DOWNLOAD_CACHE} as a content-addressed cache.
# Usage: download_to <url> <dest_path>
# ---------------------------------------------------------------------------
download_to() {
    local url="$1"
    local dest="$2"
    local cache_name
    cache_name="$(basename "${dest}")"
    local cache_path="${ORCP_DOWNLOAD_CACHE}/${cache_name}"

    if [[ -f "${cache_path}" ]]; then
        log_info "Using cached ${cache_path}"
    else
        log_info "Downloading ${url}"
        curl -fLsS --retry 5 --retry-delay 5 -o "${cache_path}.part" "${url}"
        mv "${cache_path}.part" "${cache_path}"
    fi
    cp -f "${cache_path}" "${dest}"
}

# ---------------------------------------------------------------------------
# systemd helpers
# ---------------------------------------------------------------------------
install_systemd_unit() {
    local src="$1"
    local name="$2"
    install -m 0644 -o root -g root "${src}" "/etc/systemd/system/${name}"
    systemctl daemon-reload
    log_info "Installed systemd unit ${name}"
}

ensure_dir() {
    local path="$1"
    local owner="${2:-${ORCP_USER}:${ORCP_GROUP}}"
    local mode="${3:-0755}"
    install -d -m "${mode}" -o "${owner%:*}" -g "${owner#*:}" "${path}"
}
