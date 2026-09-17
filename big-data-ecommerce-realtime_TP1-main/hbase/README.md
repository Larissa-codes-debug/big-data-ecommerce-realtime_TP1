# HBase - métricas do e-commerce

A tabela `ecommerce_metrics` recebe os resultados processados pelo Flink.

- Row key: `product_id`
- Column family `metrics`: cliques, adições ao carrinho, checkouts, valor e entregas concluídas.
- Column family `meta`: categoria e dados do último evento processado.

Consulta:

```bash
docker exec -it hbase hbase shell
scan 'ecommerce_metrics'
get 'ecommerce_metrics', 'prod-001'
```
