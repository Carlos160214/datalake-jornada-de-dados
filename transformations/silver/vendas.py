# Por que: dedup por id_venda porque a bronze é reingerida por completo a cada
# execução e pode repetir linhas entre lotes; preco_unitario/receita viram
# DECIMAL(10,2) (dinheiro); data/hora/dia_semana* decompõem data_venda para
# permitir análises temporais sem repetir a extração em cada consulta
# downstream; produto_cadastrado e venda_antes_do_cadastro são achados de
# qualidade conhecidos -- NUNCA descartados (apagar a linha mudaria a receita
# reportada), por isso são warn; os campos que definem uma venda (ids, data,
# quantidade, preço, canal) nunca podem faltar ou ser inválidos -> fail.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

DIAS_SEMANA_PT = {
    1: "Domingo", 2: "Segunda", 3: "Terça", 4: "Quarta",
    5: "Quinta", 6: "Sexta", 7: "Sábado",
}


@dp.materialized_view(
    name="vendas",
    comment="Vendas deduplicadas, com receita, decomposição temporal e flags de qualidade.",
)
@dp.expect_all_or_fail({
    "id_venda_valido": "id_venda IS NOT NULL",
    "data_venda_valida": "data_venda IS NOT NULL",
    "id_cliente_valido": "id_cliente IS NOT NULL",
    "id_produto_valido": "id_produto IS NOT NULL",
    "quantidade_valida": "quantidade > 0",
    "preco_unitario_valido": "preco_unitario > 0",
    "canal_venda_valido": "canal_venda IN ('ecommerce', 'loja_fisica')",
})
@dp.expect("produto_cadastrado_check", "produto_cadastrado")
@dp.expect("venda_depois_do_cadastro_check", "venda_depois_do_cadastro")
def vendas():
    vendas = spark.read.table("bronze.vendas")
    produtos = spark.read.table("silver.produtos").select(
        "id_produto", F.col("data_criacao").alias("data_criacao_produto")
    )

    df = vendas.dropDuplicates(["id_venda"])
    df = df.withColumn("preco_unitario", F.col("preco_unitario").cast(DecimalType(10, 2)))
    df = df.withColumn("receita", (F.col("quantidade") * F.col("preco_unitario")).cast(DecimalType(10, 2)))

    df = df.withColumn("data", F.to_date("data_venda"))
    df = df.withColumn("hora", F.hour("data_venda"))
    df = df.withColumn("dia_semana_num", F.dayofweek("data_venda"))  # Spark: domingo=1 ... sábado=7
    dia_map = F.create_map(*[x for d, n in DIAS_SEMANA_PT.items() for x in (F.lit(d), F.lit(n))])
    df = df.withColumn("dia_semana", dia_map[F.col("dia_semana_num")])

    df = df.join(produtos, on="id_produto", how="left")
    df = df.withColumn("produto_cadastrado", F.col("data_criacao_produto").isNotNull())
    df = df.withColumn(
        "venda_antes_do_cadastro",
        F.col("produto_cadastrado") & (F.col("data_venda") < F.col("data_criacao_produto")),
    )
    df = df.withColumn("venda_depois_do_cadastro", ~F.col("venda_antes_do_cadastro"))
    df = df.drop("data_criacao_produto")

    return df
