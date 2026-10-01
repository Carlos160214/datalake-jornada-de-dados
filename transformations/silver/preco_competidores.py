# Por que: dedup por (id_produto, nome_concorrente) é a granularidade real de uma
# cotação de concorrente; preco_concorrente vira DECIMAL(10,2) pelo mesmo motivo
# de produtos.preco_atual; data_coleta vira timestamp (pode chegar como texto da
# origem); preco_suspeito só MARCA cotações muito abaixo do nosso preço (erro
# provável de coleta ou produto errado) -- descartar mudaria a análise de
# concorrência, por isso é warn, não fail.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


@dp.materialized_view(
    name="preco_competidores",
    comment="Cotações de concorrentes deduplicadas, com flag de preço suspeito.",
)
@dp.expect_all_or_fail({
    "id_produto_valido": "id_produto IS NOT NULL",
    "preco_valido": "preco_concorrente > 0",
})
@dp.expect("preco_plausivel", "NOT preco_suspeito")
def preco_competidores():
    competidores = spark.read.table("bronze.preco_competidores")
    produtos = spark.read.table("silver.produtos").select("id_produto", "preco_atual")

    df = competidores.dropDuplicates(["id_produto", "nome_concorrente"])
    df = df.withColumn("preco_concorrente", F.col("preco_concorrente").cast(DecimalType(10, 2)))
    df = df.withColumn("data_coleta", F.to_timestamp("data_coleta"))

    df = df.join(produtos, on="id_produto", how="left")
    df = df.withColumn("preco_suspeito", F.col("preco_concorrente") < (F.col("preco_atual") * F.lit(0.6)))
    df = df.drop("preco_atual")

    return df
