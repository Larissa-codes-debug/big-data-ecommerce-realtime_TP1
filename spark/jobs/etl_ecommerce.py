from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, sum as spark_sum
import os
import happybase
import time


HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
HBASE_TABLE = os.getenv("HBASE_SPARK_TABLE", "ecommerce_spark_insights")


spark = (
    SparkSession.builder
    .appName("EcommerceETL")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

print("========================================")
print("E-COMMERCE SPARK ETL")
print("========================================")

HDFS_INPUT = "hdfs://namenode:8020/data/raw/ecommerce/logs"

print("Lendo dados do HDFS...")

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(HDFS_INPUT)
)

print("Schema:")
df.printSchema()

print("Total de eventos:")
total_events = df.count()
print(total_events)

if total_events == 0:
    raise RuntimeError("Nenhum evento encontrado no HDFS. Verifique o Flume antes de executar o Spark.")

clean = (
    df
    .filter(col("event_id").isNotNull())
    .filter(col("event_type").isNotNull())
    .filter(col("product_id").isNotNull())
)

clicks = clean.filter(col("event_type") == "click")
cart = clean.filter(col("event_type") == "cart")
deliveries = clean.filter(col("event_type") == "delivery_status")

print("Eventos de clique:", clicks.count())
print("Eventos de carrinho:", cart.count())
print("Eventos de entrega:", deliveries.count())

# WIDE DEPENDENCY: groupBy gera shuffle/exchange entre partições.
print("Executando agregação por produto (wide dependency)...")

clicks_by_product = (
    clicks
    .groupBy("product_id")
    .agg(count("*").alias("total_clicks"))
)

clicks_by_product.explain("formatted")
clicks_by_product.show(20, truncate=False)

sales = (
    clean
    .filter(col("total_value").isNotNull())
    .groupBy("product_id", "category")
    .agg(
        count("*").alias("events"),
        spark_sum("total_value").alias("revenue"),
        avg("unit_price").alias("average_price"),
    )
)

sales.explain("formatted")
sales.show(20, truncate=False)

# ============================================================
# HIVE
# ============================================================

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")

(
    clicks_by_product
    .write
    .mode("overwrite")
    .format("parquet")
    .saveAsTable("ecommerce.insights_cliques")
)

(
    sales
    .write
    .mode("overwrite")
    .format("parquet")
    .saveAsTable("ecommerce.insights_vendas")
)

print("Tabelas Hive criadas.")
spark.sql("SHOW TABLES IN ecommerce").show(truncate=False)

# ============================================================
# SPARK -> HBASE
# ============================================================

def connect_hbase():
    last_error = None
    for attempt in range(30):
        try:
            connection = happybase.Connection(
                host=HBASE_HOST,
                port=HBASE_PORT,
                timeout=5000,
                autoconnect=False,
            )
            connection.open()
            return connection, connection.table(HBASE_TABLE)
        except Exception as exc:
            last_error = exc
            print(f"Aguardando HBase ({attempt + 1}/30): {exc}")
            time.sleep(2)
    raise RuntimeError(f"Não foi possível conectar ao HBase: {last_error}")


connection, table = connect_hbase()

try:
    for row in clicks_by_product.collect():
        product_id = str(row["product_id"])
        table.put(
            f"clicks-{product_id}".encode(),
            {
                b"insight:type": b"clicks_by_product",
                b"insight:product_id": product_id.encode(),
                b"insight:total_clicks": str(row["total_clicks"]).encode(),
            },
        )

    for row in sales.collect():
        product_id = str(row["product_id"])
        category = str(row["category"] or "unknown")
        table.put(
            f"sales-{product_id}-{category}".encode(),
            {
                b"insight:type": b"sales_by_product",
                b"insight:product_id": product_id.encode(),
                b"insight:category": category.encode(),
                b"insight:events": str(row["events"]).encode(),
                b"insight:revenue": f'{float(row["revenue"] or 0):.2f}'.encode(),
                b"insight:average_price": f'{float(row["average_price"] or 0):.2f}'.encode(),
            },
        )
finally:
    connection.close()

print("Insights do Spark gravados no HBase.")

print("Consulta final Hive - cliques:")
spark.sql(
    """
    SELECT *
    FROM ecommerce.insights_cliques
    ORDER BY total_clicks DESC
    """
).show(20, truncate=False)

print("Consulta final Hive - vendas:")
spark.sql(
    """
    SELECT *
    FROM ecommerce.insights_vendas
    ORDER BY revenue DESC
    """
).show(20, truncate=False)

spark.stop()
