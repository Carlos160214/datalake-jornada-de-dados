# CLAUDE.md

Convenções do Databricks Asset Bundle da jornada de dados (e-commerce brasileiro),
válidas para qualquer trabalho futuro neste bundle.

## Catálogo e schemas

- Catálogo Unity Catalog: `ecommerce`. Nunca usar outro catálogo.
- Schemas: `bronze` (já existe, sobrescrita por outra ingestão), `silver`, `gold`.
- Nomes de tabela e coluna: português, snake_case, sem acento.
- No código (Python/SQL das transformações), tabelas são sempre referenciadas como
  `schema.tabela`, sem o catálogo (o catálogo é definido no nível do pipeline).

## Estrutura de arquivos

- Silver em Python: `transformations/silver/<tabela>.py`
  (`from pyspark import pipelines as dp`).
- Gold em SQL: `transformations/gold/<tabela>.sql`.
- Um arquivo por tabela.
- Cada arquivo de transformação começa com um comentário em português explicando o
  PORQUÊ das regras (não o quê — o código já mostra o quê).

## Pipeline

- Todas as tabelas são `@dp.materialized_view` com leitura batch (`spark.read.table`).
  Nunca `@dp.table` com leitura streaming (`spark.readStream`) — a bronze é
  sobrescrita por completo a cada execução de ingestão, então não há stream válido
  para consumir incrementalmente.
- Pipeline serverless, catálogo `ecommerce`, schema padrão `silver`.
- Tabelas gold são publicadas como `gold.<tabela>`.

## Qualidade de dados

- Dinheiro é sempre `DECIMAL(10,2)`.
- Um problema de qualidade **conhecido e esperado** (ex.: venda de produto não
  cadastrado, preço de concorrente suspeito) é **marcado** em uma coluna booleana e
  medido com `@dp.expect` (severidade warn). Linhas nunca são descartadas por um
  problema de qualidade conhecido — por exemplo, apagar vendas mudaria a receita
  reportada.
- `@dp.expect_all_or_fail` é reservado para invariantes verdadeiros: condições que
  nunca podem acontecer (chave nula, valor zerado/negativo em campo obrigatório,
  categoria fora do domínio permitido). Nunca usar para marcar um problema de
  qualidade esperado.

## Antes de qualquer deploy

- Sempre rodar `databricks bundle validate --strict` antes de `databricks bundle deploy`.
