# OpenWeather Medallion Pipeline

Pipeline de dados em Python para coletar dados da API OpenWeather e persistir no Azure Storage seguindo o conceito medalhao:

- `raw`: payload original da API
- `bronze`: payload com metadados e padronizacao inicial
- `silver`: tabela curada em Parquet
- `gold`: agregacao diaria por cidade em Parquet

## Arquitetura da primeira entrega

- Extração via OpenWeather (`weather` por padrao).
- Watermark gerado por execucao (`run_id`, `ingestion_ts_utc`, `ingestion_epoch`).
- Watermark gravado:
  - dentro dos registros `raw`/`bronze`
  - nas tabelas `silver`/`gold`
  - em `metadata` dos blobs no Azure
- Escrita local em `data/` e upload para Azure Blob Storage.

## Layout no Azure Storage

Dentro do container configurado (`AZURE_STORAGE_CONTAINER`), os blobs sao gravados como:

- `raw/openweather/{endpoint}/ingestion_date=YYYY-MM-DD/ingestion_hour=HH/city={cidade}/run_id={run_id}.json`
- `bronze/openweather/{endpoint}/ingestion_date=YYYY-MM-DD/ingestion_hour=HH/city={cidade}/run_id={run_id}.json`
- `silver/openweather/openweather_current_weather/ingestion_date=YYYY-MM-DD/ingestion_hour=HH/run_id={run_id}/part-00000.parquet`
- `gold/openweather/weather_city_daily_snapshot/ingestion_date=YYYY-MM-DD/ingestion_hour=HH/run_id={run_id}/part-00000.parquet`

## Configuracao

1. Crie/ajuste `config/.env` a partir de `config/.env.example`.
2. Variaveis obrigatorias:
   - `OPENWEATHER_API_KEY`
   - `AZURE_STORAGE_ACCOUNT_NAME`
   - `AZURE_STORAGE_CONTAINER`
   - Um metodo de autenticacao Azure:
     - `AZURE_STORAGE_CONNECTION_STRING`, ou
     - `AZURE_STORAGE_ACCOUNT_KEY`, ou
     - `AZURE_STORAGE_SAS_TOKEN`
3. `OPENWEATHER_CITIES` usa `;` entre cidades (ex.: `Sao Paulo,BR;Rio de Janeiro,BR`).

## Execucao

Instalar dependencias:

```bash
uv sync
```

Executar pipeline com upload Azure:

```bash
uv run python src/extract_data.py --env-file config/.env
```

Executar somente local (sem upload):

```bash
uv run python src/extract_data.py --env-file config/.env --local-only
```

## Databricks notebooks

Pacote de notebooks pronto para importar no Databricks:

- `notebooks/databricks/_common.py`
- `notebooks/databricks/00_smoke_test.py`
- `notebooks/databricks/01_raw_bronze_ingestion.py`
- `notebooks/databricks/02_silver_transform.py`
- `notebooks/databricks/03_gold_transform.py`
- `notebooks/databricks/99_controlm_entrypoint.py`

### Fluxo

0. `00_smoke_test`: valida dependencias, credenciais e escrita no storage.
1. `01_raw_bronze_ingestion`: extrai API e grava `raw` + `bronze`.
   - inclui `storage_preflight_check` para validar escrita no storage antes da carga.
2. `02_silver_transform`: transforma `bronze` em `silver` (Parquet).
3. `03_gold_transform`: agrega `silver` em `gold` (Parquet).
4. `99_controlm_entrypoint`: orquestra `01 -> 02 -> 03` e retorna JSON unico.

Observacao: os notebooks Databricks foram ajustados para modo Serverless-safe.
Eles nao dependem de `sparkContext`, `abfss` ou `spark.conf.set` para acessar storage.
A leitura/escrita no Azure e feita direto via `azure-storage-blob`.

### Como importar

1. No Databricks Workspace, crie uma pasta de projeto.
2. Importe os arquivos `.py` acima como notebooks source.
3. Execute primeiro `00_smoke_test`.
4. Depois execute `99_controlm_entrypoint`.

### Widgets e segredos

Widgets principais usados pelos notebooks:

- `p_storage_account` (default `tropowxdlprod`)
- `p_container` (default `openweather-data`)
- `p_cities` (formato com `;`, ex.: `Sao Paulo,BR;Rio de Janeiro,BR`)
- `p_openweather_endpoints` (default `weather`)
- `p_openweather_secret_scope` + `p_openweather_secret_key` (preferencial para API key)
- `p_storage_connection_string_secret_scope` + `p_storage_connection_string_secret_key` (opcional)
- `p_storage_secret_scope` + `p_storage_secret_key` (preferencial para account key)
- `p_storage_sas_secret_scope` + `p_storage_sas_secret_key` (opcional)
- `p_allow_plaintext_credentials` (default `false`; manter assim em producao)
- `p_stage_timeout_seconds` (somente no `99`, default `0`)

### Credenciais no job (recomendado)

Nao use `p_api_key`, `p_storage_account_key`, `p_storage_connection_string` ou `p_storage_sas_token` em producao.

Use segredo por scope/key ou Spark conf no Job:

1. `pipeline.openweather.api_key`
2. `pipeline.storage.connection_string` ou `pipeline.storage.account_key` ou `pipeline.storage.sas_token`

Prioridade de leitura das credenciais:

1. Spark conf
2. Secret scope/key (widgets)
3. Plaintext widget (somente com `p_allow_plaintext_credentials=true`)

### Teste manual sem expor credencial em widget

Para teste manual no Workspace:

1. Deixe `p_allow_plaintext_credentials=false`.
2. Preencha somente:
   - `p_openweather_secret_scope` + `p_openweather_secret_key`
   - `p_storage_connection_string_secret_scope` + `p_storage_connection_string_secret_key` (se usar connection string), ou
   - `p_storage_secret_scope` + `p_storage_secret_key`
   - `p_storage_sas_secret_scope` + `p_storage_sas_secret_key` (se usar SAS)
3. Execute `99_controlm_entrypoint`.

Somente se necessario para debug rapido:

- use `p_api_key` e um metodo storage (`p_storage_account_key` ou `p_storage_connection_string` ou `p_storage_sas_token`) com `p_allow_plaintext_credentials=true`.

### Dependencias do cluster

Garanta no cluster/serverless environment:

1. `azure-storage-blob`
2. `pyarrow`
3. `pandas`
4. `requests`

Obs.: o notebook `_common.py` tenta auto-instalar `azure-storage-blob` e `pyarrow` caso nao encontre.

## Control-M orchestration

Recomendacao especialista: o Control-M chama apenas o notebook `99_controlm_entrypoint`.

Ordem tecnica:

1. Control-M dispara Databricks Job (Jobs API / Run Now).
2. Job executa `99_controlm_entrypoint`.
3. `99` encadeia os demais notebooks.
4. Control-M captura o JSON de saida para auditoria e alertas.

Se seu cluster ja tem permissao no storage (Managed Identity / Service Principal), o `p_storage_account_key` pode ficar vazio.

Template de payload para `Jobs API 2.1 /jobs/run-now`:

- `notebooks/databricks/controlm_run_now_payload.example.json`

## Troubleshooting

Os notebooks de stage (`00`, `01`, `02`, `03`) retornam JSON com:

- `status`
- `error_type`
- `error_message`
- `traceback`

Se o `99_controlm_entrypoint` falhar, a mensagem ja inclui a causa raiz retornada pelo stage.
