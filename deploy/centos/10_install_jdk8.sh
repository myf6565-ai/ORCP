#!/usr/bin/env bash
#
# 10_install_jdk8.sh --- 安装 Eclipse Temurin JDK 8。
# 参见 DEV_SPEC §5.2。
#
# 默认安装 8u402-b06。需了解的上游文件名规律：
#   发布标签  : jdk8u402-b06
#   压缩包名  : OpenJDK8U-jdk_x64_linux_hotspot_8u402b06.tar.gz（更新号与构建号之间无分隔符）
#   解压根目录: jdk8u402-b06
#
# 变量覆盖：
#   JDK_UPDATE=8u402  JDK_BUILD=b06   使用其他 Temurin 发布版本
#   ORCP_JDK_URL=https://...           完全覆盖下载地址
#
# 幂等性：/opt/jdk-8 已存在且有效时跳过下载。

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

require_root
load_cluster_env

: "${JDK_UPDATE:=8u402}"
: "${JDK_BUILD:=b06}"

JDK_TAG="jdk${JDK_UPDATE}-${JDK_BUILD}"                         # 例：jdk8u402-b06
JDK_ASSET="OpenJDK8U-jdk_x64_linux_hotspot_${JDK_UPDATE}${JDK_BUILD}.tar.gz"
JDK_URL="${ORCP_JDK_URL:-https://github.com/adoptium/temurin8-binaries/releases/download/${JDK_TAG}/${JDK_ASSET}}"

INSTALL_ROOT=/opt
TARGET_LINK="${INSTALL_ROOT}/jdk-8"
EXTRACT_DIR="${INSTALL_ROOT}/${JDK_TAG}"

if [[ -x "${TARGET_LINK}/bin/java" ]] \
   && "${TARGET_LINK}/bin/java" -version 2>&1 | grep -q '"1.8'; then
    log_info "JDK 8 已安装于 ${TARGET_LINK}，跳过下载"
else
    log_info "下载 Temurin ${JDK_TAG}"
    TMP_TAR="${ORCP_DOWNLOAD_CACHE}/${JDK_ASSET}"
    download_to "${JDK_URL}" "${TMP_TAR}"

    log_info "解压到 ${INSTALL_ROOT}"
    tar -xzf "${TMP_TAR}" -C "${INSTALL_ROOT}"

    rm -f "${TARGET_LINK}"
    ln -s "${EXTRACT_DIR}" "${TARGET_LINK}"
fi

log_info "写入 /etc/profile.d/jdk.sh"
cat >/etc/profile.d/jdk.sh <<EOF
# 由 ORCP deploy/centos/10_install_jdk8.sh 管理
export JAVA_HOME=${TARGET_LINK}
export PATH=\$JAVA_HOME/bin:\$PATH
export JAVA_TOOL_OPTIONS="\${JAVA_TOOL_OPTIONS:-} -Dfile.encoding=UTF-8"
EOF
chmod 0644 /etc/profile.d/jdk.sh

log_info "向 alternatives 注册"
if command -v alternatives >/dev/null 2>&1; then
    alternatives --install /usr/bin/java  java  "${TARGET_LINK}/bin/java"  20000 \
                 --slave /usr/bin/javac javac "${TARGET_LINK}/bin/javac" \
                 --slave /usr/bin/jar   jar   "${TARGET_LINK}/bin/jar"
    alternatives --set java "${TARGET_LINK}/bin/java"
fi

log_info "验证安装结果"
# shellcheck disable=SC1091
source /etc/profile.d/jdk.sh
java -version
log_info "10_install_jdk8.sh 执行完毕"
