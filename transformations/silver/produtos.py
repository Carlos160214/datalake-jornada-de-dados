# Por que: dedup por id_produto porque a bronze pode repetir produto entre
# ingestões; preco_atual vira DECIMAL(10,2) porque é dinheiro (double arredonda
# errado); faixa_preco evita repetir a lógica de segmentação em cada consulta
# downstream; id_produto/preco_atual inválidos nunca podem acontecer -> fail,
# não warn.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


@dp.materialized_view(
    name="produtos",
    comment="Produtos deduplicados, com preço tipado e faixa de preço.",
)
@dp.expect_all_or_fail({
    "id_produto_valido": "id_produto IS NOT NULL",
    "preco_valido": "preco_atual > 0",
})
def produtos():
    df = spark.read.table("bronze.produtos")

    df = df.dropDuplicates(["id_produto"])
    df = df.withColumn("nome_produto", F.trim("nome_produto"))
    df = df.withColumn("preco_atual", F.col("preco_atual").cast(DecimalType(10, 2)))

    df = df.withColumn(
        "faixa_preco",
        F.when(F.col("preco_atual") > 1000, F.lit("PREMIUM"))
         .when(F.col("preco_atual") > 500, F.lit("MEDIO"))
         .otherwise(F.lit("BASICO")),
    )

    return df
