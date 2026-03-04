# OpenWeather Medallion Pipeline

Pipeline de dados para ingestao OpenWeather e persistencia no Azure Blob Storage com arquitetura medalhao:

- `raw`: payload original da API + watermark
- `bronze`: payload semiestruturado com colunas padronizadas
- `silver`: dataset curado em Parquet
- `gold`: agregacao diaria por cidade em Parquet

## Arquitetura de execucao

Entrada oficial em Databricks:

- `notebooks/databricks/98_full_pipeline_no_widgets.py`

Suporte tecnico (debug e desenvolvimento):

- `notebooks/databricks/00_smoke_test.py`
- `notebooks/databricks/01_raw_bronze_ingestion.py`
- `notebooks/databricks/02_silver_transform.py`
- `notebooks/databricks/03_gold_transform.py`
- `notebooks/databricks/_common.py`

## Padrao de armazenamento

Os blobs sao gravados em:

- `raw/openweather/{endpoint}/ingestion_date=YYYY-MM-DD/ingestion_hour=HH/city={slug}/run_id={run_id}.json`
- `bronze/openweather/{endpoint}/ingestion_date=YYYY-MM-DD/ingestion_hour=HH/city={slug}/run_id={run_id}.json`
- `silver/openweather/openweather_current_weather/ingestion_date=YYYY-MM-DD/ingestion_hour=HH/run_id={run_id}/part-00000.parquet`
- `gold/openweather/weather_city_daily_snapshot/ingestion_date=YYYY-MM-DD/ingestion_hour=HH/run_id={run_id}/part-00000.parquet`
- `raw/openweather/_control/runs/{ingestion_epoch}_{run_id}.json` (manifest)

## Execucao local (Python)

Instalar dependencias:

```bash
uv sync
```

Rodar pipeline Python:

```bash
uv run python src/extract_data.py --env-file config/.env
```

Pre-check antes de push:

```bash
uv run python scripts/pre_push_checks.py
```

## Databricks (modo profissional)

Runbook operacional:

- `docs/OPERACAO_DATABRICKS.md`
- `docs/CHECKLIST_QUALIDADE.md`
- `docs/GIT_VERSIONAMENTO.md`
- `docs/CICD.md`

### 1. Secret scope

Crie um scope e chaves (exemplo):

- scope: `kv-openweather`
- key OpenWeather: `openweather-api-key`
- key Storage: `storage-account-key`

### 2. Configurar o notebook 98

No arquivo `notebooks/databricks/98_full_pipeline_no_widgets.py`, configure:

1. `ACTIVE_PROFILE`
2. `PIPELINE_PROFILES`
3. `LOCAL_OVERRIDE_FILE` (opcional para Free Edition)

Padrao recomendado:

- `allow_plaintext_credentials = False`
- `manual_config.openweather_api_key = secret://kv-openweather/openweather-api-key`
- `manual_config.storage_auth_mode = account_key`
- `manual_config.storage_credential = secret://kv-openweather/storage-account-key`

### 2.1 Databricks Free Edition (sem secret scope)

Use o profile `free_plaintext_local` no notebook 98 e evite gravar chave no Git:

1. Copie `config/databricks_free.local.example.json` para `config/databricks_free.local.json`.
2. Preencha as chaves reais no arquivo local.
3. Mantenha `ACTIVE_PROFILE = "free_plaintext_local"` no notebook 98.
4. Mantenha `LOCAL_OVERRIDE_FILE = "config/databricks_free.local.json"`.

Observacao: `config/databricks_free.local.json` ja esta no `.gitignore`.

### 3. Validacao manual inicial

1. Rode `00_smoke_test.py`.
2. Rode `98_full_pipeline_no_widgets.py`.
3. Verifique o JSON final com `status = ok`.

### 4. Job Databricks (producao)

Crie um Job com:

1. Notebook task apontando para `98_full_pipeline_no_widgets.py`.
2. Compute Serverless.
3. `max_concurrent_runs = 1`.
4. Retry de 2-3 tentativas.
5. Timeout de 30 minutos.
6. Schedule conforme custo/SLAs (ex.: a cada 6 horas no inicio).

Observacao: como o notebook 98 e no-widget, a configuracao fica versionada no codigo do notebook.
No painel da task, a secao `Parameters` pode ficar vazia.

## Credenciais e seguranca

Prioridade de autenticacao:

1. `secret://<scope>/<key>` (recomendado)
2. plaintext (somente com `allow_plaintext_credentials=True`)

Metodos suportados para storage:

1. `account_key`
2. `connection_string`
3. `sas_token`

Nunca comite credenciais reais no repositorio.

## Observabilidade e governanca

Cada run retorna JSON com:

- `run_id`
- contagem de registros por camada
- caminhos gerados
- tempos por etapa (`extract`, `silver`, `gold`, `total`)

Cada erro retorna JSON com:

- `error_type`
- `error_message`
- `traceback`

Qualidade (gate nativo):

- `quality_report.passed = true`
- `quality_report.failed_checks` vazio
- regras configuraveis via `manual_config.quality_rules`

## Custos (Azure Free Credit)

Para comecar com controle de custo:

1. Execute a cada 6 horas.
2. Monitore budget e alertas no Azure Cost Management.
3. Revise frequencia so depois de 1-2 semanas de consumo observado.
