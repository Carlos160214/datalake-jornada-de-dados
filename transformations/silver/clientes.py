# Por que: nome_original preserva o dado bruto (a limpeza de nome é heurística e
# pode errar); removemos pronome de tratamento e aplicamos title-case para
# padronizar relatórios; estado vira maiúsculas (é código UF, não texto livre);
# nome_estado/regiao vêm de um mapeamento fixo das 27 UFs do IBGE porque não
# existe tabela de estados na bronze; id_cliente/regiao nunca podem ficar nulos
# -> fail.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

UF_INFO = {
    "AC": ("Acre", "Norte"), "AP": ("Amapá", "Norte"), "AM": ("Amazonas", "Norte"),
    "PA": ("Pará", "Norte"), "RO": ("Rondônia", "Norte"), "RR": ("Roraima", "Norte"),
    "TO": ("Tocantins", "Norte"),
    "AL": ("Alagoas", "Nordeste"), "BA": ("Bahia", "Nordeste"), "CE": ("Ceará", "Nordeste"),
    "MA": ("Maranhão", "Nordeste"), "PB": ("Paraíba", "Nordeste"), "PE": ("Pernambuco", "Nordeste"),
    "PI": ("Piauí", "Nordeste"), "RN": ("Rio Grande do Norte", "Nordeste"), "SE": ("Sergipe", "Nordeste"),
    "DF": ("Distrito Federal", "Centro-Oeste"), "GO": ("Goiás", "Centro-Oeste"),
    "MS": ("Mato Grosso do Sul", "Centro-Oeste"), "MT": ("Mato Grosso", "Centro-Oeste"),
    "ES": ("Espírito Santo", "Sudeste"), "MG": ("Minas Gerais", "Sudeste"),
    "RJ": ("Rio de Janeiro", "Sudeste"), "SP": ("São Paulo", "Sudeste"),
    "PR": ("Paraná", "Sul"), "RS": ("Rio Grande do Sul", "Sul"), "SC": ("Santa Catarina", "Sul"),
}
TITULOS_RE = r"^(Sr\.|Sra\.|Srta\.|Dr\.|Dra\.)\s+"


@dp.materialized_view(
    name="clientes",
    comment="Clientes deduplicados, com nome normalizado e região do IBGE.",
)
@dp.expect_all_or_fail({
    "id_cliente_valido": "id_cliente IS NOT NULL",
    "regiao_valida": "regiao IS NOT NULL",
})
def clientes():
    df = spark.read.table("bronze.clientes")

    df = df.dropDuplicates(["id_cliente"])
    df = df.withColumn("nome_original", F.col("nome_cliente"))
    df = df.withColumn(
        "nome_cliente",
        F.initcap(F.regexp_replace(F.trim(F.col("nome_cliente")), TITULOS_RE, "")),
    )
    df = df.withColumn("estado", F.upper(F.trim(F.col("estado"))))

    estado_map = F.create_map(*[x for uf, (nome, _) in UF_INFO.items() for x in (F.lit(uf), F.lit(nome))])
    regiao_map = F.create_map(*[x for uf, (_, reg) in UF_INFO.items() for x in (F.lit(uf), F.lit(reg))])
    df = df.withColumn("nome_estado", estado_map[F.col("estado")])
    df = df.withColumn("regiao", regiao_map[F.col("estado")])

    return df
