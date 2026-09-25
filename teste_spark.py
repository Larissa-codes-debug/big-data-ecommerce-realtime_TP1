from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TesteSpark")
    .master("local[*]")
    .getOrCreate()
)

dados = [
    ("produto_1", 10.0),
    ("produto_2", 20.0),
    ("produto_3", 30.0),
]

df = spark.createDataFrame(
    dados,
    ["produto_id", "valor"]
)

print("===== TESTE SPARK =====")
print("Quantidade de registros:", df.count())

df.show()

print("===== SPARK FUNCIONANDO =====")

spark.stop()
