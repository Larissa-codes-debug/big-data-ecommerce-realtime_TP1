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

create_table_if_needed() {
    local table="$1"
    local families="$2"

    while true; do
        echo "create '$table', $families" | hbase shell -n > /tmp/hbase-create.log 2>&1 || true
        cat /tmp/hbase-create.log

        if grep -q "Created table $table" /tmp/hbase-create.log; then
            echo "Tabela $table criada com sucesso."
            break
        fi

        if grep -q "already exists" /tmp/hbase-create.log; then
            echo "Tabela $table já existe."
            break
        fi

        echo "Master ainda não aceitou a criação de $table. Tentando novamente em 5 segundos..."
        sleep 5
    done
}

create_table_if_needed "ecommerce_metrics" "'metrics', 'meta', 'alerts'"
create_table_if_needed "ecommerce_spark_insights" "'insight'"

echo "======================================"
echo "HBase inicializado com sucesso."
echo "Tabelas:"
echo "  - ecommerce_metrics (metrics, meta, alerts)"
echo "  - ecommerce_spark_insights (insight)"
echo "======================================"
