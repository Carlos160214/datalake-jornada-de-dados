# Databricks notebook source
# MAGIC %md
# MAGIC # Testes de qualidade da camada silver
# MAGIC
# MAGIC Cada teste é uma consulta que conta linhas com problema. Se qualquer teste
# MAGIC encontrar linhas, o notebook falha com `AssertionError` e mostra uma tabela
# MAGIC com o resultado de cada teste.

# COMMAND ----------

dbutils.widgets.text("catalogo", "ecommerce")
catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------

TABELAS_CHAVE = {
    "produtos": ["id_produto"],
    "clientes": ["id_cliente"],
    "preco_competidores": ["id_produto", "nome_concorrente"],
    "vendas": ["id_venda"],
}

resultados = []

# Teste 1: chaves únicas das 4 tabelas silver.
for tabela, chave in TABELAS_CHAVE.items():
    colunas = ", ".join(chave)
    qtd = spark.sql(f"""
        SELECT COUNT(*) AS qtd FROM (
            SELECT {colunas}
            FROM {catalogo}.silver.{tabela}
            GROUP BY {colunas}
            HAVING COUNT(*) > 1
        )
    """).collect()[0]["qtd"]
    resultados.append((f"chave_unica_{tabela}", float(qtd), qtd == 0))

# Teste 2: receita = quantidade * preco_unitario em silver.vendas.
qtd_receita = spark.sql(f"""
    SELECT COUNT(*) AS qtd
    FROM {catalogo}.silver.vendas
    WHERE receita <> CAST(quantidade * preco_unitario AS DECIMAL(10, 2))
""").collect()[0]["qtd"]
resultados.append(("receita_igual_quantidade_vezes_preco", float(qtd_receita), qtd_receita == 0))

# Teste 3: vendas de produto não cadastrado abaixo de 1% do total.
pct_nao_cadastrado = spark.sql(f"""
    SELECT
        SUM(CASE WHEN NOT produto_cadastrado THEN 1 ELSE 0 END) / COUNT(*) AS pct
    FROM {catalogo}.silver.vendas
""").collect()[0]["pct"]
resultados.append(("vendas_produto_nao_cadastrado_abaixo_1pct", float(pct_nao_cadastrado), pct_nao_cadastrado < 0.01))

# COMMAND ----------

df_resultados = spark.createDataFrame(resultados, ["teste", "valor", "passou"])
display(df_resultados)

# COMMAND ----------

falhas = [nome for nome, _, passou in resultados if not passou]
if falhas:
    raise AssertionError(f"Testes de qualidade falharam: {', '.join(falhas)}")
