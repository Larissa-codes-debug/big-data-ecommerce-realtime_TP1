#!/bin/bash

set -e

echo "======================================"
echo "HBase Init"
echo "======================================"

echo "Configuracao do ZooKeeper:"
grep -A1 "hbase.zookeeper.quorum" /opt/hbase/conf/hbase-site.xml

echo "Aguardando HBase..."

until echo "status" | hbase shell -n 2>&1 | grep -q "active master"; do
    echo "HBase ainda nao esta pronto..."
    sleep 5
done

echo "HBase disponivel."

echo "Criando tabela ecommerce_metrics..."

while true; do

    echo "create 'ecommerce_metrics', 'metrics', 'meta', 'alert'" \
        | hbase shell -n > /tmp/hbase-create.log 2>&1 || true

    cat /tmp/hbase-create.log

    if grep -q "Created table ecommerce_metrics" /tmp/hbase-create.log; then
        echo "Tabela ecommerce_metrics criada com sucesso."
        break
    fi

    if grep -q "already exists" /tmp/hbase-create.log; then
        echo "Tabela ecommerce_metrics ja existe."
        break
    fi

    echo "Master ainda nao aceitou a criacao."
    echo "Tentando novamente em 5 segundos..."
    sleep 5

done

echo "======================================"
echo "HBase inicializado com sucesso."
echo "Tabela: ecommerce_metrics"
echo "======================================"