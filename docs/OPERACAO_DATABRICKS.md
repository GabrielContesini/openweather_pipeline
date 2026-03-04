# Operacao Databricks - OpenWeather Medallion

## Objetivo

Padronizar a operacao de producao usando somente o notebook:

- `notebooks/databricks/98_full_pipeline_no_widgets.py`

## Pre-requisitos

1. Secret scope criado no Databricks (exemplo: `kv-openweather`).
2. Secrets criados:
- `openweather-api-key`
- `storage-account-key`
3. Container existente no Azure Blob (`openweather-data`).

## Primeira execucao manual

1. Abra o notebook `98_full_pipeline_no_widgets.py`.
2. Ajuste apenas a celula `PIPELINE_SETTINGS`.
3. Use `secret://<scope>/<key>` para credenciais.
4. Rode `Run all`.
5. Valide o JSON final com `status = ok`.

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

## Sinais de sucesso

O notebook precisa retornar JSON com:

- `status: ok`
- `run_id`
- `raw_records`, `bronze_records`, `silver_rows`, `gold_rows`
- `manifest_path`
- `timings.total_seconds`

## Troubleshooting rapido

1. Erro de secret: valide scope/key e permissao de leitura em `dbutils.secrets`.
2. Erro de storage auth: confirme `storage_auth_mode` e secret correto.
3. Erro 401/403 OpenWeather: valide API key e limite da conta.
4. Erro 429/5xx OpenWeather: o pipeline ja tenta retry automatico; se persistir, reexecute.
