# Enterprise Foundations - OpenWeather Pipeline

## Scope

This document covers four senior-level capabilities added to the project:

1. Infrastructure as Code (Terraform)
2. Observability with centralized logs and SLA gates
3. Governance baseline with Unity Catalog and ACLs
4. Delta incremental, idempotent upsert, and backfill strategy

## 1) Infrastructure as Code

Location:

- `infra/terraform`

Main resources:

1. Log Analytics Workspace
2. Action Group (alerts)
3. Diagnostic settings:
- Storage -> Log Analytics
- Databricks -> Log Analytics
4. Metric alerts:
- Storage availability
- Storage egress

Optional:

- create storage account/container when `create_storage_resources=true`

## 2) Observability + SLA

Implemented in `notebooks/databricks/_common.py` and consumed by notebook `98`.

Capabilities:

1. `quality_report` with enforceable checks
2. `sla_report` with thresholds for extract/silver/gold/total duration
3. Observability event persisted in storage:
- `raw/openweather/_control/observability/.../run_id=<run_id>.json`
4. Manifest enriched with:
- timings
- quality_report
- sla_report
- delta_report
- run_status / failure_reasons

## 3) Governance (Unity Catalog)

Bootstrap notebook:

- `notebooks/databricks/110_uc_governance_bootstrap.py`

What it does:

1. Creates catalog and schemas
2. Creates base Delta tables (silver/gold/checkpoint)
3. Creates consumer views
4. Applies grants for engineers/analysts/scientists

Notes:

1. Adjust group names in `UC_SETTINGS.principals`.
2. If some groups do not exist yet, notebook returns warnings.

## 4) Delta incremental and backfill

Incremental in online pipeline:

1. Enable `manual_config.delta_config.enabled=true` in notebook `98`.
2. Pipeline writes blobs and also performs Delta MERGE for silver/gold.
3. Schema evolution enabled by config (`merge_schema=true`).

Backfill from historical bronze:

- `notebooks/databricks/120_delta_backfill_from_bronze.py`

Backfill controls:

1. `start_date`
2. `end_date`
3. `run_ids` (optional)
4. `max_runs`

## Recommended execution order

1. Apply Terraform (`infra/terraform`)
2. Run UC bootstrap (`110_uc_governance_bootstrap`)
3. Enable Delta in notebook `98` profile
4. Run notebook `98` for incremental
5. Run notebook `120` for historical backfill
