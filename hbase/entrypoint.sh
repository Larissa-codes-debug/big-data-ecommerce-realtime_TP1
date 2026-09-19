#!/bin/bash

set -e

mkdir -p /data/hbase

echo "Iniciando ZooKeeper..."

hbase-daemon.sh start zookeeper

sleep 5

echo "Iniciando HMaster..."

hbase-daemon.sh start master

sleep 5

echo "Iniciando RegionServer..."

hbase-daemon.sh start regionserver

echo "Iniciando Thrift..."

hbase-daemon.sh start thrift -p 9090

echo "HBase iniciado. Thrift: 9090"

trap 'hbase-daemon.sh stop thrift || true; hbase-daemon.sh stop regionserver || true; hbase-daemon.sh stop master || true; hbase-daemon.sh stop zookeeper || true' TERM INT

while true; do
    sleep 30
done