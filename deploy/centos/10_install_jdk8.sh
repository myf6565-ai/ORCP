#!/usr/bin/env bash
#
# 10_install_jdk8.sh --- Install Eclipse Temurin JDK 8.
# See DEV_SPEC §5.2.
#
# Defaults to 8u402-b06. Upstream filename quirks to be aware of:
#   release tag   : jdk8u402-b06
#   asset name    : OpenJDK8U-jdk_x64_linux_hotspot_8u402b06.tar.gz  (no "-" between the update and the build)
#   extract root  : jdk8u402-b06
#
# Overrides:
#   JDK_UPDATE=8u402  JDK_BUILD=b06         to use a different Temurin release
#   ORCP_JDK_URL=https://...                to override the download URL entirely
#
# Idempotent: re-running is a no-op once /opt/jdk-8 is valid.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

: "${JDK_UPDATE:=8u402}"
: "${JDK_BUILD:=b06}"

JDK_TAG="jdk${JDK_UPDATE}-${JDK_BUILD}"                         # e.g. jdk8u402-b06
JDK_ASSET="OpenJDK8U-jdk_x64_linux_hotspot_${JDK_UPDATE}${JDK_BUILD}.tar.gz"
JDK_URL="${ORCP_JDK_URL:-https://github.com/adoptium/temurin8-binaries/releases/download/${JDK_TAG}/${JDK_ASSET}}"

INSTALL_ROOT=/opt
TARGET_LINK="${INSTALL_ROOT}/jdk-8"
EXTRACT_DIR="${INSTALL_ROOT}/${JDK_TAG}"

if [[ -x "${TARGET_LINK}/bin/java" ]] \
   && "${TARGET_LINK}/bin/java" -version 2>&1 | grep -q '"1.8'; then
    log_info "JDK 8 already installed at ${TARGET_LINK}; skipping download"
else
    log_info "Downloading Temurin ${JDK_TAG}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${JDK_ASSET}"
    download_to "${JDK_URL}" "${TMP_TAR}"

    log_info "Extracting to ${INSTALL_ROOT}"
    tar -xzf "${TMP_TAR}" -C "${INSTALL_ROOT}"

    rm -f "${TARGET_LINK}"
    ln -s "${EXTRACT_DIR}" "${TARGET_LINK}"
fi

log_info "Writing /etc/profile.d/jdk.sh"
cat >/etc/profile.d/jdk.sh <<EOF
# Managed by ORCP deploy/centos/10_install_jdk8.sh
export JAVA_HOME=${TARGET_LINK}
export PATH=\$JAVA_HOME/bin:\$PATH
export JAVA_TOOL_OPTIONS="\${JAVA_TOOL_OPTIONS:-} -Dfile.encoding=UTF-8"
EOF
chmod 0644 /etc/profile.d/jdk.sh

log_info "Registering with alternatives"
if command -v alternatives >/dev/null 2>&1; then
    alternatives --install /usr/bin/java  java  "${TARGET_LINK}/bin/java"  20000 \
                 --slave /usr/bin/javac javac "${TARGET_LINK}/bin/javac" \
                 --slave /usr/bin/jar   jar   "${TARGET_LINK}/bin/jar"
    alternatives --set java "${TARGET_LINK}/bin/java"
fi

log_info "Verifying installation"
# shellcheck disable=SC1091
source /etc/profile.d/jdk.sh
java -version
log_info "10_install_jdk8.sh finished successfully"
