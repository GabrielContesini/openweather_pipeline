# Git Versionamento - Padrao do Projeto

## Estrategia de branches

1. `main`: branch estavel de producao.
2. `feature/<tema>`: novas funcionalidades.
3. `fix/<tema>`: correcoes de defeito.
4. `hotfix/<tema>`: correcao urgente em producao.

## Convencao de commit

Use mensagens no estilo Conventional Commits:

1. `feat: ...`
2. `fix: ...`
3. `docs: ...`
4. `refactor: ...`
5. `chore: ...`

Exemplos:

1. `feat: add quality gate to full pipeline notebook`
2. `fix: handle openweather transient http errors`
3. `docs: add databricks free operational runbook`

## Regras de pull request

1. PR pequeno e focado em um objetivo.
2. Descrever risco de impacto em producao.
3. Rodar pre-check local:
- `uv run python scripts/pre_push_checks.py`
4. Incluir evidencias:
- JSON de saida com `status = ok`
- caminhos de `manifest_path`, `silver_blob_path` e `gold_blob_path`
5. Nao versionar export bruto `.html` do Databricks.
6. Nunca incluir credenciais reais.

## Tags e releases

1. Tag por marco funcional:
- `v0.1.0`, `v0.2.0`, `v1.0.0`
2. Patch para correcoes:
- `v0.2.1`, `v0.2.2`
3. Registrar no release notes:
- alteracoes
- risco
- plano de rollback
