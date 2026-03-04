# OpenWeather Medallion Pipeline

Pipeline de dados para ingestao OpenWeather e persistencia no Azure Blob Storage com arquitetura medalhao:

- `raw`: payload original da API + watermark
- `bronze`: payload semiestruturado com colunas padronizadas
- `silver`: dataset curado em Parquet
- `gold`: agregacao diaria por cidade em Parquet

## Documentacao central

- `docs/PROJECT_STRUCTURE.md`
- `docs/OPERACAO_DATABRICKS.md`
- `docs/CHECKLIST_QUALIDADE.md`
- `docs/CICD.md`
- `docs/ENTERPRISE_FOUNDATIONS.md`
- `docs/EVIDENCIAS_EXECUCAO.md`

## Arquitetura de execucao

Entrada oficial em Databricks:

- `notebooks/databricks/98_full_pipeline_no_widgets.py`

Suporte tecnico (debug e desenvolvimento):

- `notebooks/databricks/00_smoke_test.py`
- `notebooks/databricks/01_raw_bronze_ingestion.py`
- `notebooks/databricks/02_silver_transform.py`
- `notebooks/databricks/03_gold_transform.py`
- `notebooks/databricks/110_uc_governance_bootstrap.py`
- `notebooks/databricks/120_delta_backfill_from_bronze.py`
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

Gerar resumo de evidencias a partir de export HTML do Databricks:

```bash
uv run python scripts/extract_databricks_run_summary.py <caminho_do_html>
```

## Databricks Free Edition (recomendado para este projeto)

Runbooks:

- `docs/OPERACAO_DATABRICKS.md`
- `docs/CHECKLIST_QUALIDADE.md`
- `docs/GIT_VERSIONAMENTO.md`
- `docs/CICD.md`
- `docs/ENTERPRISE_FOUNDATIONS.md`
- `docs/PROJECT_STRUCTURE.md`
- `docs/EVIDENCIAS_EXECUCAO.md`

Infra as Code:

- `infra/terraform`

### 1. Configurar credenciais no modo Free

1. Copie `config/databricks_free.local.example.json` para `config/databricks_free.local.json`.
2. Preencha `openweather_api_key` e `storage_credential` no arquivo local.
3. O arquivo local ja esta ignorado no Git (`.gitignore`).

### 2. Configurar o notebook 98

No arquivo `notebooks/databricks/98_full_pipeline_no_widgets.py`, configure:

1. `ACTIVE_PROFILE = "free_plaintext_local"`
2. `LOCAL_OVERRIDE_FILE = "config/databricks_free.local.json"`
3. `manual_config.delta_config.enabled = true` quando quiser gravar Delta incremental

### 3. Validacao manual inicial

1. Rode `00_smoke_test.py`.
2. Rode `98_full_pipeline_no_widgets.py`.
3. Verifique `status = ok`, `quality_report.passed = true` e `sla_report.passed = true`.

### 4. Job Databricks

Crie um Job com:

1. Notebook task apontando para `98_full_pipeline_no_widgets.py`.
2. `max_concurrent_runs = 1`.
3. Retry de 2-3 tentativas.
4. Timeout de 30 minutos.
5. Schedule conforme custo (ex.: a cada 6 horas no inicio).
6. `Parameters` vazio (notebook no-widget).

### 5. Recursos enterprise opcionais (workspace pago)

1. Secret scope (`secret://...`) para remover plaintext.
2. Unity Catalog + ACL/linhagem usando notebook `110`.
3. Diagnostics de workspace Databricks no Azure Monitor.

## Credenciais e seguranca

Prioridade de autenticacao:

1. `secret://<scope>/<key>` (workspace pago)
2. plaintext com arquivo local (Free Edition)

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

SLA (gate nativo):

- `sla_report.passed = true`
- regras configuraveis via `manual_config.sla_rules`
- evento operacional em `_control/observability`

## Custos (Azure Free Credit)

Para comecar com controle de custo:

1. Execute a cada 6 horas.
2. Monitore budget e alertas no Azure Cost Management.
3. Revise frequencia so depois de 1-2 semanas de consumo observado.

## Evidencia de run atual

- `docs/evidence/databricks_runs/2026-03-04/98_full_pipeline_no_widgets.summary.json`
