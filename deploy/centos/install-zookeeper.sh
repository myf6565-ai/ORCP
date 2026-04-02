#!/usr/bin/env bash
set -euo pipefail
VERSION=${1:-3.9.2}
curl -LO https://archive.apache.org/dist/zookeeper/zookeeper-${VERSION}/apache-zookeeper-${VERSION}-bin.tar.gz
tar -xzf apache-zookeeper-${VERSION}-bin.tar.gz
