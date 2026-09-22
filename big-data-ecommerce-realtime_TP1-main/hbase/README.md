# HBase - métricas e alertas do e-commerce

A tabela `ecommerce_metrics` recebe as métricas acumuladas e os alertas processados pelo Flink.

- Row key de métricas: `product_id`
- Row key de alertas: `alert|product_id|window_start|window_end`
- Column families: `metrics`, `meta` e `alert`

Consulta:

```bash
docker compose exec hbase hbase shell
scan 'ecommerce_metrics'
get 'ecommerce_metrics', 'prod-001'
```
