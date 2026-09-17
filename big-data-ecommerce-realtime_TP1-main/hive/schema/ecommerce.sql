-- ===========================================================================
-- SCHEMA ECOMMERCE - PROCESSAMENTO EM LOTE (BATCH) NO HIVE
-- ===========================================================================

-- 1. Criação da Tabela Externa (Raw Data) apontando para os dados do Flume no HDFS
-- O Flume está configurado para salvar os dados em hdfs://namenode:8020/data/raw/ecommerce/logs/
CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs (
    event_id STRING,
    event_type STRING,
    product_id STRING,
    category STRING,
    cart_action STRING,
    total_value DOUBLE,
    delivery_status STRING,
    event_time STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION 'hdfs://namenode:8020/data/raw/ecommerce/logs/';


-- ===========================================================================
-- CONSULTAS ANALÍTICAS EM BATCH
-- As consultas abaixo representam o processamento em lote dos dados de e-commerce.
-- ===========================================================================

-- 2. Tabela de Métricas Agregadas por Produto (semelhante ao resultado do Flink)
CREATE TABLE IF NOT EXISTS product_metrics_batch AS
SELECT
    product_id,
    category,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
    SUM(CASE WHEN event_type = 'cart' AND cart_action = 'add' THEN 1 ELSE 0 END) AS total_cart_adds,
    SUM(CASE WHEN event_type = 'cart' AND cart_action = 'checkout' THEN 1 ELSE 0 END) AS total_checkouts,
    SUM(CASE WHEN event_type = 'cart' AND cart_action = 'checkout' THEN total_value ELSE 0.0 END) AS total_revenue,
    SUM(CASE WHEN event_type = 'delivery_status' AND delivery_status = 'delivered' THEN 1 ELSE 0 END) AS total_delivered
FROM
    raw_logs
WHERE 
    product_id IS NOT NULL
GROUP BY
    product_id, category;

-- 3. Exemplo de Consulta Simples (Top 5 Produtos mais vendidos)
-- SELECT product_id, category, total_checkouts, total_revenue 
-- FROM product_metrics_batch 
-- ORDER BY total_checkouts DESC 
-- LIMIT 5;