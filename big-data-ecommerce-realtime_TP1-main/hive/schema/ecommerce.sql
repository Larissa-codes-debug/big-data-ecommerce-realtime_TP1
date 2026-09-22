
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

