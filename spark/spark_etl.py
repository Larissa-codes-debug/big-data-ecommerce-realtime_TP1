"""
spark_etl.py
------------
Frente Spark do pipeline (Bianca ).

Le os eventos brutos gravados no HDFS (mesmo schema do gerador.py:
click / cart / delivery_status), faz ETL em batch com operacoes de
*wide dependency* (agregacoes com groupBy e um join, que forcam shuffle
entre particoes) e grava o resultado em tabelas Hive para a Larissa
consumir na camada de Data Warehouse.

USO:
    spark-submit spark_etl.py \
        --input hdfs://namenode:8020/raw/events \
        --hive-db colmeia

Se --input ou --hive-db nao forem passados, usa os defaults abaixo.
Ajuste DEFAULT_INPUT_PATH assim que confirmar o path real na pasta hdfs/.
"""

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# DEFAULTS — ajustar depois de confirmar com a pasta hdfs/ do repositório
# ---------------------------------------------------------------------------
DEFAULT_INPUT_PATH = "hdfs://namenode:8020/raw/events"
DEFAULT_HIVE_DB = "colmeia"  # nome do banco Hive (pode trocar se a Larissa usar outro)

# Schema explícito == mesmos campos do gerador.py (evita o Spark ter que
# inferir o schema lendo o arquivo inteiro, que é lento e pode falhar
# quando chegam campos nulos).
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("schema_version", StringType(), True),
        StructField("event_type", StringType(), True),       # click | cart | delivery_status
        StructField("event_time", StringType(), True),       # ISO8601 UTC
        StructField("ingested_at", StringType(), True),       # ISO8601 UTC
        StructField("source", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_value", DoubleType(), True),
        StructField("cart_action", StringType(), True),       # add | remove | checkout
        StructField("order_id", StringType(), True),
        StructField("delivery_status", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL batch Spark -> Hive")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH,
                         help="caminho HDFS dos eventos brutos (JSONL)")
    parser.add_argument("--hive-db", default=DEFAULT_HIVE_DB,
                         help="database Hive de destino")
    return parser.parse_args()


def load_events(spark: SparkSession, input_path: str):
    """Le o JSONL bruto do HDFS, tipa as colunas e remove duplicados
    (o event_id é único por definição no gerador)."""

    df = (
        spark.read.schema(EVENT_SCHEMA)
        .json(input_path)
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .dropDuplicates(["event_id"])
    )
    return df


# ---------------------------------------------------------------------------
# TRANSFORMACOES — cada uma abaixo tem pelo menos uma wide dependency
# (groupBy ou join provocam shuffle: os dados são reparticionados pela
# chave de agrupamento/junção, diferente de um map/filter que é narrow).
# ---------------------------------------------------------------------------

def resumo_por_categoria(df):
    """WIDE: groupBy + agregações -> shuffle por 'category'."""
    cliques = df.filter(F.col("event_type") == "click")
    vendas = df.filter(
        (F.col("event_type") == "cart") & (F.col("cart_action") == "checkout")
    )

    cliques_por_cat = cliques.groupBy("category").agg(
        F.count("*").alias("total_cliques")
    )
    vendas_por_cat = vendas.groupBy("category").agg(
        F.count("*").alias("total_pedidos"),
        F.sum("total_value").alias("receita_total"),
        F.avg("total_value").alias("ticket_medio"),
    )

    return cliques_por_cat.join(vendas_por_cat, on="category", how="outer").fillna(0)


def taxa_abandono_carrinho(df):
    """WIDE: groupBy por session_id, depois outro groupBy por categoria."""
    carrinho = df.filter(F.col("event_type") == "cart")

    por_sessao = carrinho.groupBy("session_id", "category").agg(
        F.max(F.when(F.col("cart_action") == "add", 1).otherwise(0)).alias("teve_add"),
        F.max(F.when(F.col("cart_action") == "checkout", 1).otherwise(0)).alias("teve_checkout"),
    )

    resultado = por_sessao.groupBy("category").agg(
        F.sum("teve_add").alias("sessoes_com_add"),
        F.sum("teve_checkout").alias("sessoes_com_checkout"),
    ).withColumn(
        "taxa_abandono",
        F.when(F.col("sessoes_com_add") > 0,
               1 - (F.col("sessoes_com_checkout") / F.col("sessoes_com_add")))
        .otherwise(F.lit(None)),
    )
    return resultado


def funil_entrega(df):
    """WIDE: groupBy simples por status."""
    entregas = df.filter(F.col("event_type") == "delivery_status")
    return entregas.groupBy("delivery_status").agg(
        F.countDistinct("order_id").alias("total_pedidos")
    )


def pedidos_x_status_atual(df):
    """WIDE: join entre pedidos (originados no cart/checkout) e o status
    de entrega mais recente de cada order_id -> junção clássica com shuffle."""
    pedidos = (
        df.filter((F.col("event_type") == "cart") & (F.col("cart_action") == "checkout"))
        .select("order_id", "user_id", "category", "total_value", "event_time")
        .withColumnRenamed("event_time", "checkout_time")
    )

    status_recente = (
        df.filter(F.col("event_type") == "delivery_status")
        .groupBy("order_id")
        .agg(F.max_by("delivery_status", "event_time").alias("status_atual"))
    )

    return pedidos.join(status_recente, on="order_id", how="left")


def escrever_hive(df, spark: SparkSession, hive_db: str, tabela: str):
    """Grava (overwrite) uma tabela Hive gerenciada dentro do database dado."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    nome_completo = f"{hive_db}.{tabela}"
    df.write.mode("overwrite").saveAsTable(nome_completo)
    print(f"[spark_etl] tabela {nome_completo} escrita com {df.count()} linhas")


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("ecommerce-realtime-etl")
        .enableHiveSupport()
        .getOrCreate()
    )

    print(f"[spark_etl] lendo eventos de: {args.input}")
    eventos = load_events(spark, args.input)
    eventos.cache()
    print(f"[spark_etl] total de eventos lidos: {eventos.count()}")

    escrever_hive(resumo_por_categoria(eventos), spark, args.hive_db, "resumo_categoria")
    escrever_hive(taxa_abandono_carrinho(eventos), spark, args.hive_db, "abandono_carrinho")
    escrever_hive(funil_entrega(eventos), spark, args.hive_db, "funil_entrega")
    escrever_hive(pedidos_x_status_atual(eventos), spark, args.hive_db, "pedidos_status_atual")

    spark.stop()


if __name__ == "__main__":
    main()
