#!/usr/bin/env bash
set -euo pipefail
VERSION=${1:-3.7.1}
curl -LO https://archive.apache.org/dist/kafka/${VERSION}/kafka_2.13-${VERSION}.tgz
tar -xzf kafka_2.13-${VERSION}.tgz
