#!/usr/bin/env bash
set -euo pipefail

echo "A submissão automática é feita pelo serviço flink-job."
echo "Use: docker compose logs -f flink-job"
echo "Para consultar os jobs: docker compose exec -T flink-jobmanager flink list -m flink-jobmanager:8081"
