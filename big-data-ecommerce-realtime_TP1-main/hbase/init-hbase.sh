#!/bin/bash
set -e
until nc -z hbase 9090; do
  echo "Aguardando HBase Thrift..."
  sleep 2
done
sleep 2
hbase shell <<'HBASE'
create 'ecommerce_metrics', 'metrics', 'meta'
HBASE
