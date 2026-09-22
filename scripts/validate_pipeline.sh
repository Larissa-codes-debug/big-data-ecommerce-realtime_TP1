#!/usr/bin/env bash
set -euo pipefail

echo "== Containers =="
docker compose ps

echo
echo "== Flink overview =="
docker compose exec -T flink-jobmanager curl -fsS http://localhost:8081/overview

echo
echo "== Flink jobs =="
docker compose exec -T flink-jobmanager flink list

echo
echo "== HDFS files =="
docker compose exec -T namenode hdfs dfs -ls /data/raw/ecommerce/logs || true

echo
echo "== HBase table =="
docker compose exec -T hbase hbase shell -n <<'EOF'
list
scan 'ecommerce_metrics', {LIMIT => 5}
EOF

echo
echo "Validação básica concluída."
