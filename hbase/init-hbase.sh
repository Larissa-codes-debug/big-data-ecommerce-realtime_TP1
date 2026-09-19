#!/bin/bash

set -e

HBASE_CONF="/opt/hbase/conf/hbase-site.xml"

echo "Configurando HBase Init..."

sed -i \
    '/<name>hbase.zookeeper.quorum<\/name>/,/<\/property>/ {
        /<value>/ s#<value>.*</value>#<value>hbase</value>#
    }' \
    "$HBASE_CONF"

echo "ZooKeeper configurado para: hbase"

echo "Aguardando HBase..."

until echo "status" | hbase shell -n 2>&1 | grep -q "active master"; do
    echo "HBase ainda não está pronto..."
    sleep 5
done

echo "HBase disponível."

echo "Criando tabela ecommerce_metrics..."

while true; do

    echo "create 'ecommerce_metrics', 'metrics'" | hbase shell -n > /tmp/hbase-create.log 2>&1 || true

    cat /tmp/hbase-create.log

    if grep -q "Created table ecommerce_metrics" /tmp/hbase-create.log; then
        echo "Tabela ecommerce_metrics criada com sucesso."
        break
    fi

    if grep -q "Table already exists" /tmp/hbase-create.log; then
        echo "Tabela ecommerce_metrics já existe."
        break
    fi

    echo "Master ainda não aceitou a criação. Tentando novamente em 5 segundos..."
    sleep 5

done

echo "======================================"
echo "HBase inicializado com sucesso."
echo "Tabela: ecommerce_metrics"
echo "======================================"