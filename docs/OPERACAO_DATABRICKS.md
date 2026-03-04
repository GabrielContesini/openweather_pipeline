# Operacao Databricks - OpenWeather Medallion

## Objetivo

Padronizar a operacao de producao usando somente o notebook:

- `notebooks/databricks/98_full_pipeline_no_widgets.py`

## Pre-requisitos

1. Databricks Free workspace com notebook `98`.
2. Arquivo local `config/databricks_free.local.json` com chaves reais.
3. Container existente no Azure Blob (`openweather-data`).

## Primeira execucao manual

1. Abra o notebook `98_full_pipeline_no_widgets.py`.
2. Defina `ACTIVE_PROFILE = "free_plaintext_local"`.
3. Defina `LOCAL_OVERRIDE_FILE = "config/databricks_free.local.json"`.
4. Rode `Run all`.
5. Valide o JSON final com:
- `status = ok`
- `quality_report.passed = true`
- `sla_report.passed = true`

## Configuracao de Job (producao)

1. Job name: `job_openweather_medallion_prd`.
2. Task type: Notebook.
3. Notebook path: `.../98_full_pipeline_no_widgets`.
4. Retries: 2 ou 3.
5. Timeout: 30 min.
6. Schedule inicial: a cada 6 horas.
7. `max_concurrent_runs = 1`.
8. Task `Parameters`: deixar vazio (notebook no-widget).

## Databricks Free Edition

1. Copie `config/databricks_free.local.example.json` para `config/databricks_free.local.json`.
2. Preencha as chaves reais no arquivo local.
3. No notebook 98, use:
- `ACTIVE_PROFILE = "free_plaintext_local"`
- `LOCAL_OVERRIDE_FILE = "config/databricks_free.local.json"`
4. Rode manualmente e valide `quality_report.passed = true`.

## Notebooks enterprise adicionais

1. `110_uc_governance_bootstrap.py`:
- opcional para workspace pago com Unity Catalog
- no Free, pode retornar `result = skipped`
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
