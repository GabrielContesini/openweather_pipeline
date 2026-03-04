# Estrutura do Projeto

## Objetivo

Padronizar como o repositorio deve crescer, mantendo operacao simples no Databricks Free Edition e pronto para evolucao enterprise.

## Estrutura atual

```text
openweather_pipeline/
|- config/                   # Configuracoes locais (arquivos reais ignorados no Git)
|- docs/                     # Runbooks, arquitetura e operacao
|  |- evidence/              # Evidencias de execucao (preferencia: JSON resumido)
|- infra/terraform/          # IaC para observabilidade e alertas no Azure
|- notebooks/databricks/     # Orquestracao e estagios do pipeline
|- scripts/                  # Automacao local, CI/CD e utilitarios
|- src/                      # Implementacao Python para execucao local
|- tests/                    # Testes unitarios
|- README.md                 # Guia principal
|- pyproject.toml            # Dependencias Python
```

## Regras de organizacao

1. Notebook oficial de producao: `notebooks/databricks/98_full_pipeline_no_widgets.py`.
2. Notebooks auxiliares (`00`, `01`, `02`, `03`, `110`, `120`) ficam como suporte tecnico.
3. Evidencia versionada deve ser compacta (JSON de resumo), nao HTML bruto.
4. Qualquer credencial real fica fora do Git (`config/databricks_free.local.json`, `terraform.tfvars`, `.env`).
5. Mudancas de infra devem ficar isoladas em `infra/terraform`.
6. Todo push deve passar por `scripts/pre_push_checks.py`.

## Convencoes de nomes

1. Notebooks: prefixo numerico com 2 ou 3 digitos (`98_...`, `120_...`).
2. Arquivos de evidencia: `<notebook>.summary.json`.
3. Documentacao: caixa alta com underscore para runbooks principais (`OPERACAO_DATABRICKS.md`).
4. Scripts Python: verbo + objetivo (`deploy_databricks_workspace.py`, `extract_databricks_run_summary.py`).
