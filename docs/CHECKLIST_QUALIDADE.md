# Checklist de Qualidade - OpenWeather Medallion

## Antes de rodar

1. Confirmar que o notebook oficial e o `98_full_pipeline_no_widgets.py`.
2. Confirmar `ACTIVE_PROFILE` correto no notebook 98.
3. Confirmar credenciais:
- `prod_secret_scope`: `secret://<scope>/<key>`
- `free_plaintext_local`: arquivo local `config/databricks_free.local.json`
4. Validar cidades e endpoints em `manual_config`.
5. Validar `quality_rules`:
- `enforce_exact_input_count = true`
- `min_silver_rows >= 1`
- `min_gold_rows >= 1`

## Durante a execucao

1. Validar que o notebook retornou `status = ok`.
2. Validar `quality_report.passed = true`.
3. Validar contagens:
- `raw_records`
- `bronze_records`
- `silver_rows`
- `gold_rows`
4. Validar `timings.total_seconds` dentro da janela esperada.

## Depois da execucao

1. Conferir escrita de blobs:
- `storage_preflight_path`
- `silver_blob_path`
- `gold_blob_path`
- `manifest_path`
2. Conferir manifest em `raw/openweather/_control/runs/`.
3. Registrar `run_id` para auditoria.
4. Em caso de erro, guardar `error_type`, `error_message` e `traceback`.

## Gate minimo de aprovacao

Uma execucao e considerada aprovada apenas quando:

1. `status = ok`.
2. `quality_report.passed = true`.
3. `raw_records == bronze_records`.
4. `silver_rows >= min_silver_rows`.
5. `gold_rows >= min_gold_rows`.
