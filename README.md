# datalake-jornada-de-dados

Repositório da jornada de dados: ingestão, tratamento e disponibilização de dados em camadas (bronze → silver → gold).

## Estrutura

```
docs/        Arquitetura, decisões e dicionário de dados
sql/
  bronze/    Dados brutos, como chegam da origem
  silver/    Dados limpos, padronizados e integrados
  gold/      Modelos prontos para consumo (BI, relatórios)
pipelines/   Scripts de ingestão e ETL
notebooks/   Exploração e análises
config/      Modelos de configuração (sem segredos)
```

## Configuração

1. Copie `config/.env.example` para `.env` na raiz e preencha as credenciais.
2. O `.env`, wallets, chaves e arquivos de dados são ignorados pelo Git (ver `.gitignore`).

## Fluxo de trabalho

- Commits pequenos e com mensagem clara.
- Nunca versionar credenciais nem dados.
