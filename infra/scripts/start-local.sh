#!/usr/bin/env bash
set -euo pipefail

docker compose -f infra/docker-compose/docker-compose.yml up -d
echo "本地 Kafka 环境已启动"
