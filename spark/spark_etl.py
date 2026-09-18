"""
spark_etl.py
------------
Frente Spark do pipeline (Bianca / "Pessoa 5").

Le a tabela externa `raw_logs` (ja criada no Hive pela Larissa, apontando
pra hdfs://namenode:8020/data/raw/ecommerce/logs/), faz o ETL em batch
com uma agregacao de *wide dependency* (groupBy por product_id/category,
que forca shuffle entre particoes) e grava o resultado na tabela Hive
`product_metrics_batch` -- a mesma tabela/colunas que a Larissa ja
definiu em hive/schema/ecommerce.sql, só que calculada pelo Spark em vez
de HiveQL puro (é essa a diferenca que justifica a camada Spark na
arquitetura: ETL feito em Spark, não direto no Hive).

USO (dentro do container/spark-submit):
    spark-submit spark_etl.py

Nao precisa de argumentos: o path de entrada ja vem do Hive metastore
(porque raw_logs é external table). Se quiser sobrescrever o database,
use --hive-db.
"""

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL batch Spark -> Hive")
    parser.add_argument(
        "--hive-db",
        default="default",
        help="database Hive onde raw_logs/product_metrics_batch vivem (default: 'default')",
    )
    return parser.parse_args()


def carregar_raw_logs(spark: SparkSession, hive_db: str):
    """Le a tabela externa raw_logs direto do Hive metastore (o Spark
    enxerga o mesmo catalogo, por isso nao precisamos repetir o path
    do HDFS aqui -- ele ja esta na definicao da tabela)."""

    spark.sql(f"USE {hive_db}")
    return spark.table("raw_logs")


def calcular_product_metrics(df):
    """WIDE DEPENDENCY: groupBy por (product_id, category) forca o Spark
    a reparticionar e embaralhar (shuffle) os dados entre os executores
    pela chave de agrupamento -- diferente de um select/filter (narrow),
    que processa cada particao isoladamente."""

    return (
        df.filter(F.col("product_id").isNotNull())
        .groupBy("product_id", "category")
        .agg(
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias(
                "total_clicks"
            ),
            F.sum(
                F.when(
                    (F.col("event_type") == "cart") & (F.col("cart_action") == "add"),
                    1,
                ).otherwise(0)
            ).alias("total_cart_adds"),
            F.sum(
                F.when(
                    (F.col("event_type") == "cart")
                    & (F.col("cart_action") == "checkout"),
                    1,
                ).otherwise(0)
            ).alias("total_checkouts"),
            F.sum(
                F.when(
                    (F.col("event_type") == "cart")
                    & (F.col("cart_action") == "checkout"),
                    F.col("total_value"),
                ).otherwise(0.0)
            ).alias("total_revenue"),
            F.sum(
                F.when(
                    (F.col("event_type") == "delivery_status")
                    & (F.col("delivery_status") == "delivered"),
                    1,
                ).otherwise(0)
            ).alias("total_delivered"),
        )
    )


def escrever_hive(df, spark: SparkSession, tabela: str):
    """Sobrescreve a tabela Hive de destino com o resultado do Spark."""
    df.write.mode("overwrite").saveAsTable(tabela)
    print(f"[spark_etl] tabela '{tabela}' escrita com {df.count()} linhas")


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("ecommerce-product-metrics-etl")
        .enableHiveSupport()
        .getOrCreate()
    )

    print("[spark_etl] lendo raw_logs do Hive metastore...")
    raw_logs = carregar_raw_logs(spark, args.hive_db)
    raw_logs.cache()
    print(f"[spark_etl] total de linhas lidas em raw_logs: {raw_logs.count()}")

    metrics = calcular_product_metrics(raw_logs)
    escrever_hive(metrics, spark, "product_metrics_batch")

    spark.stop()


if __name__ == "__main__":
    main()
