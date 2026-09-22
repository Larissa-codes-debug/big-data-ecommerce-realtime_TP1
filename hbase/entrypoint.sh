#!/bin/bash

set -e

echo "======================================"
echo "Iniciando HBase"
echo "======================================"

mkdir -p /data/hbase

echo "Configuracao do ZooKeeper:"
grep -A1 "hbase.zookeeper.quorum" /opt/hbase/conf/hbase-site.xml || true

echo "Iniciando ZooKeeper..."
hbase-daemon.sh start zookeeper

sleep 5

echo "Verificando ZooKeeper..."
if ! nc -z localhost 2181; then
    echo "ERRO: ZooKeeper nao iniciou na porta 2181."
    exit 1
fi

echo "ZooKeeper OK."

echo "Iniciando HMaster..."
hbase-daemon.sh start master

sleep 10

echo "Iniciando RegionServer..."
hbase-daemon.sh start regionserver

sleep 10

echo "Iniciando Thrift..."
hbase-daemon.sh start thrift -p 9090

sleep 5

echo "======================================"
echo "HBase iniciado."
echo "ZooKeeper: 2181"
echo "Master: 16010"
echo "Thrift: 9090"
echo "======================================"

trap '
    hbase-daemon.sh stop thrift || true
    hbase-daemon.sh stop regionserver || true
    hbase-daemon.sh stop master || true
    hbase-daemon.sh stop zookeeper || true
' TERM INT

while true; do
    sleep 30
done