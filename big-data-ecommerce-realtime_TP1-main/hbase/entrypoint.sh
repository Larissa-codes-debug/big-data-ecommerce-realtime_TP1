#!/bin/bash
set -e
mkdir -p /data/hbase
hbase-daemon.sh start master
hbase-daemon.sh start regionserver
hbase-daemon.sh start thrift -p 9090

echo "HBase iniciado. Thrift: 9090"
trap 'hbase-daemon.sh stop thrift || true; hbase-daemon.sh stop regionserver || true; hbase-daemon.sh stop master || true' TERM INT
while true; do sleep 30; done
