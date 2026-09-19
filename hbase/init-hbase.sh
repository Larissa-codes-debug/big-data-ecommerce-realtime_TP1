#!/bin/bash

set -e

HBASE_CONF="/opt/hbase/conf/hbase-site.xml"

echo "Configurando HBase Init..."

# O ZooKeeper está no container hbase.
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

echo "Verificando tabela ecommerce_metrics..."

until echo "list" | hbase shell -n 2>/dev/null | grep -q "ecommerce_metrics"; do
    echo "Tabela ainda não está disponível. Aguardando..."
    sleep 5
done

echo "Tabela ecommerce_metrics encontrada."
echo "Inicialização do HBase concluída."