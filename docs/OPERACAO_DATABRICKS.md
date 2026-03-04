# Operacao Databricks - OpenWeather Medallion

## Objetivo

Padronizar a operacao de producao usando somente o notebook:

- `notebooks/databricks/98_full_pipeline_no_widgets.py`

## Pre-requisitos

1. Para `prod_secret_scope`: secret scope criado no Databricks (exemplo: `kv-openweather`).
2. Para `prod_secret_scope`: secrets criados:
- `openweather-api-key`
- `storage-account-key`
3. Para `free_plaintext_local`: arquivo local `config/databricks_free.local.json`.
4. Container existente no Azure Blob (`openweather-data`).

## Primeira execucao manual

1. Abra o notebook `98_full_pipeline_no_widgets.py`.
2. Defina `ACTIVE_PROFILE`.
3. Ajuste o bloco do profile em `PIPELINE_PROFILES`.
4. Use `secret://<scope>/<key>` para credenciais em producao.
5. Se estiver em Databricks Free, use `free_plaintext_local` com arquivo local ignorado no Git.
6. Opcional: defina `LOCAL_OVERRIDE_FILE = "config/databricks_free.local.json"`.
7. Rode `Run all`.
8. Valide o JSON final com `status = ok`.

## Configuracao de Job (producao)

1. Job name: `job_openweather_medallion_prd`.
2. Task type: Notebook.
3. Notebook path: `.../98_full_pipeline_no_widgets`.
4. Compute: Serverless.
5. Retries: 2 ou 3.
6. Timeout: 30 min.
7. Schedule inicial: a cada 6 horas.
8. `max_concurrent_runs = 1`.
9. Task `Parameters`: deixar vazio (notebook no-widget).

## Databricks Free Edition

1. Copie `config/databricks_free.local.example.json` para `config/databricks_free.local.json`.
2. Preencha as chaves reais no arquivo local.
3. No notebook 98, use:
- `ACTIVE_PROFILE = "free_plaintext_local"`
- `LOCAL_OVERRIDE_FILE = "config/databricks_free.local.json"`
4. Rode manualmente e valide `quality_report.passed = true`.

## Notebooks enterprise adicionais

1. `110_uc_governance_bootstrap.py`:
- cria catalog/schema/tabelas/views e grants de governanca
2. `120_delta_backfill_from_bronze.py`:
- executa backfill historico para Delta com MERGE idempotente

## Sinais de sucesso

O notebook precisa retornar JSON com:

- `status: ok`
- `run_id`
- `raw_records`, `bronze_records`, `silver_rows`, `gold_rows`
- `manifest_path`
- `timings.total_seconds`
- `quality_report.passed`

## Troubleshooting rapido

1. Erro de secret: valide scope/key e permissao de leitura em `dbutils.secrets`.
2. Erro de storage auth: confirme `storage_auth_mode` e secret correto.
3. Erro 401/403 OpenWeather: valide API key e limite da conta.
4. Erro 429/5xx OpenWeather: o pipeline ja tenta retry automatico; se persistir, reexecute.
