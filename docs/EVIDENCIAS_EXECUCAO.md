# Evidencias de Execucao

## Objetivo

Registrar execucoes do pipeline de forma auditavel sem expor segredos ou inflar o repositorio.

## Padrao recomendado

1. Exportar notebook run em HTML somente para uso local.
2. Gerar resumo sanitizado em JSON.
3. Versionar apenas o JSON no Git.

Pasta padrao:

- `docs/evidence/databricks_runs/<YYYY-MM-DD>/`

## Como gerar resumo a partir do HTML exportado

Comando:

```bash
uv run python scripts/extract_databricks_run_summary.py <caminho_do_html>
```

Exemplo:

```bash
uv run python scripts/extract_databricks_run_summary.py docs/evidence/databricks_runs/2026-03-04/98_full_pipeline_no_widgets.html
```

O script cria automaticamente:

- `docs/evidence/databricks_runs/2026-03-04/98_full_pipeline_no_widgets.summary.json`

## Por que nao versionar HTML

1. Export HTML embute notebook completo.
2. Pode carregar dados sensiveis (chaves, paths locais, dados temporarios).
3. Gera ruido em diff e aumenta o tamanho do repo.

## Campos chave do summary

1. `status`, `stage`, `run_id`
2. `record_counts`
3. `quality.passed` e `sla.passed`
4. `timings`
5. `paths` (manifest e blobs)

## Evidencia atual

- `docs/evidence/databricks_runs/2026-03-04/98_full_pipeline_no_widgets.summary.json`
