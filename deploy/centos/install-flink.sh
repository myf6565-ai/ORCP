#!/usr/bin/env bash
set -euo pipefail
VERSION=${1:-1.18.1}
curl -LO https://archive.apache.org/dist/flink/flink-${VERSION}/flink-${VERSION}-bin-scala_2.12.tgz
tar -xzf flink-${VERSION}-bin-scala_2.12.tgz
