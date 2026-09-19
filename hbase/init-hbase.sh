#!/bin/bash

set -e

echo "Aguardando HBase..."

until echo "status" | hbase shell >/dev/null 2>&1
do
    sleep 5
done

echo "HBase disponível."

echo "Criando tabela ecommerce_metrics..."

echo "create 'ecommerce_metrics', 'metrics'" | hbase shell

echo "Tabela criada."