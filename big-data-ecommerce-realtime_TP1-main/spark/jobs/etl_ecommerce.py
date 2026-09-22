from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg
)


spark = (
    SparkSession.builder
    .appName("EcommerceETL")
    .config(
        "spark.sql.warehouse.dir",
        "hdfs://namenode:8020/user/hive/warehouse"
    )
    .enableHiveSupport()
    .getOrCreate()
)


print("========================================")
print("E-COMMERCE SPARK ETL")
print("========================================")


HDFS_INPUT = (
    "hdfs://namenode:8020/"
    "data/raw/ecommerce/logs/*.json"
)


print("Lendo dados do HDFS...")

df = (
    spark.read
    .json(HDFS_INPUT)
)


print("Schema:")

df.printSchema()


print("Total de eventos:")

print(df.count())


# ============================================================
# LIMPEZA
# ============================================================

clean = (
    df
    .filter(col("event_id").isNotNull())
    .filter(col("event_type").isNotNull())
)


# ============================================================
# CLICKS
# ============================================================

clicks = (
    clean
    .filter(
        col("event_type") == "click"
    )
)


clicks.show(10, truncate=False)


# ============================================================
# CARRINHO
# ============================================================

cart = (
    clean
    .filter(
        col("event_type") == "cart"
    )
)


# ============================================================
# ENTREGAS
# ============================================================

deliveries = (
    clean
    .filter(
        col("event_type") == "delivery_status"
    )
)


# ============================================================
# WIDE DEPENDENCY
# ============================================================

print("Executando agregação por produto...")

clicks_by_product = (
    clicks
    .groupBy(
        "product_id"
    )
    .agg(
        count("*").alias("total_clicks")
    )
)


clicks_by_product.show(
    20,
    truncate=False
)


# ============================================================
# INSIGHT DE VENDAS
# ============================================================

sales = (
    clean
    .filter(
        col("total_value").isNotNull()
    )
    .groupBy(
        "product_id",
        "category"
    )
    .agg(
        count("*").alias("events"),
        spark_sum("total_value").alias("revenue"),
        avg("unit_price").alias("average_price")
    )
)


sales.show(
    20,
    truncate=False
)


# ============================================================
# HIVE
# ============================================================

spark.sql(
    "CREATE DATABASE IF NOT EXISTS ecommerce"
)


(
    clicks_by_product
    .write
    .mode("overwrite")
    .saveAsTable(
        "ecommerce.insights_cliques"
    )
)


(
    sales
    .write
    .mode("overwrite")
    .saveAsTable(
        "ecommerce.insights_vendas"
    )
)


print("Tabelas Hive criadas.")

spark.sql(
    "SHOW TABLES IN ecommerce"
).show(
    truncate=False
)


print("Consulta final:")

spark.sql(
    """
    SELECT *
    FROM ecommerce.insights_cliques
    ORDER BY total_clicks DESC
    """
).show(
    20,
    truncate=False
)


spark.stop()