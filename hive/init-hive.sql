CREATE DATABASE IF NOT EXISTS ecommerce;

USE ecommerce;

CREATE TABLE IF NOT EXISTS insights_cliques (
    product_id STRING,
    total_clicks BIGINT
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS insights_vendas (
    product_id STRING,
    category STRING,
    events BIGINT,
    revenue DOUBLE,
    average_price DOUBLE
)
STORED AS PARQUET;