# CI/CD - OpenWeather Medallion

## Visao geral

O projeto agora possui dois workflows GitHub Actions:

1. `ci`:
- roda em `pull_request` e `push` (`main` e `develop`)
- executa checks de qualidade e testes
2. `cd-databricks-workspace`:
- roda em `push` para `main` (quando ha mudanca em notebooks/scripts de deploy)
- tambem pode ser disparado manualmente (`workflow_dispatch`)
- faz deploy dos notebooks para Databricks Workspace via API REST
3. `iac-terraform-validate`:
- roda em PR/push quando houver mudanca em `infra/terraform`
- executa `terraform fmt`, `init -backend=false` e `validate`

## Arquivos de workflow

1. `.github/workflows/ci.yml`
2. `.github/workflows/cd-databricks-workspace.yml`
3. `.github/workflows/iac-terraform-validate.yml`

## O que o CI valida

1. Instalacao de dependencias (`uv sync --frozen`).
2. `scripts/pre_push_checks.py`:
- compilacao dos arquivos Python criticos
- deteccao basica de credencial em texto no notebook 98
- validacao de arquivos sensiveis rastreados no Git
3. Testes unitarios (`python -m unittest`).

## O que o CD faz

1. Le notebooks de `notebooks/databricks`.
2. Converte cada arquivo em payload base64.
3. Importa notebooks no Databricks via `POST /api/2.0/workspace/import`.
4. Sobrescreve notebooks existentes (`overwrite=true`).

Script usado:

- `scripts/deploy_databricks_workspace.py`

## Secrets e variaveis no GitHub

No repositorio do GitHub, configure:

Secrets (obrigatorios):

1. `DATABRICKS_HOST`
- Exemplo: `https://adb-1234567890123456.7.azuredatabricks.net`
2. `DATABRICKS_TOKEN`
- Personal Access Token (PAT) com permissao de workspace import

Variables (opcional):

1. `DATABRICKS_TARGET_PATH`
- Default do script:
`/Shared/openweather_pipeline/notebooks/databricks`

## Como gerar PAT no Databricks Free

1. Abrir Databricks.
2. Ir em `User Settings`.
3. Abrir aba `Developer`.
4. Criar token em `Access tokens`.
5. Copiar valor e salvar em `DATABRICKS_TOKEN` no GitHub.

## Primeira ativacao recomendada

1. Subir branch com os workflows.
2. Abrir PR e validar `ci` verde.
3. Fazer merge em `main`.
4. Rodar `cd-databricks-workspace` manualmente (`Run workflow`) na primeira vez.
5. Confirmar notebooks no path de destino no Databricks.

## Boas praticas

1. Rotacionar `DATABRICKS_TOKEN` periodicamente.
2. Proteger branch `main` exigindo `ci` aprovado.
3. Usar environment `production` no workflow de CD com aprovacao manual, se desejar gate humano.
4. Nao armazenar credenciais reais em arquivos versionados.
