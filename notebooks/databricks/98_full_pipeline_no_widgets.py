# Databricks notebook source
# MAGIC %md
# MAGIC # 98 - Full Pipeline (No Widgets, Production Path)
# MAGIC
# MAGIC Official pipeline entrypoint for Databricks Jobs and manual execution.
# MAGIC
# MAGIC Profiles supported:
# MAGIC 1. `prod_secret_scope` (recommended)
# MAGIC 2. `free_plaintext_local` (Databricks Free fallback)
# MAGIC
# MAGIC Credential options:
# MAGIC 1. `secret://<scope>/<key>`
# MAGIC 2. plaintext (only if `allow_plaintext_credentials=True`)
# MAGIC
# MAGIC Optional local override file:
# MAGIC - `config/databricks_free.local.json` (ignored in Git)

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
# EDIT ONLY THIS CELL
import json
from pathlib import Path
from typing import Any


ACTIVE_PROFILE = "free_plaintext_local"
LOCAL_OVERRIDE_FILE = "config/databricks_free.local.json"

PIPELINE_PROFILES = {
    "prod_secret_scope": {
        "allow_plaintext_credentials": False,
        "create_container_if_missing": True,
        "manual_config": {
            "storage_account": "tropowxdlprod",
            "container": "openweather-data",
            "storage_auth_mode": "account_key",
            "storage_credential": "secret://kv-openweather/storage-account-key",
            "openweather_api_key": "secret://kv-openweather/openweather-api-key",
            "openweather_base_url": "https://api.openweathermap.org/data/2.5",
            "openweather_endpoints": ["weather"],
            "openweather_units": "metric",
            "openweather_lang": "pt_br",
            "openweather_timeout_seconds": 30,
            "cities": ["Sao Paulo,BR", "Rio de Janeiro,BR", "Curitiba,BR"],
            "quality_rules": {
                "enforce_exact_input_count": True,
                "min_silver_rows": 1,
                "min_gold_rows": 1,
                "fail_on_quality_violation": True,
            },
            "sla_rules": {
                "max_total_seconds": 900,
                "max_extract_seconds": 600,
                "max_silver_seconds": 240,
                "max_gold_seconds": 240,
                "min_raw_records": 1,
                "require_quality_passed": True,
                "fail_on_sla_violation": True,
            },
            "delta_config": {
                "enabled": False,
                "catalog": "weather_prd",
                "schema": "weather_ops",
                "silver_table": "openweather_current_weather_delta",
                "gold_table": "weather_city_daily_snapshot_delta",
                "checkpoint_table": "openweather_pipeline_checkpoint",
                "merge_schema": True,
                "create_schema_if_missing": True,
            },
        },
    },
    "free_plaintext_local": {
        "allow_plaintext_credentials": True,
        "create_container_if_missing": True,
        "manual_config": {
            "storage_account": "tropowxdlprod",
            "container": "openweather-data",
            "storage_auth_mode": "account_key",
            "storage_credential": "<STORAGE_ACCOUNT_KEY>",
            "openweather_api_key": "<OPENWEATHER_API_KEY>",
            "openweather_base_url": "https://api.openweathermap.org/data/2.5",
            "openweather_endpoints": ["weather"],
            "openweather_units": "metric",
            "openweather_lang": "pt_br",
            "openweather_timeout_seconds": 30,
            "cities": ["Sao Paulo,BR", "Rio de Janeiro,BR", "Curitiba,BR"],
            "quality_rules": {
                "enforce_exact_input_count": True,
                "min_silver_rows": 1,
                "min_gold_rows": 1,
                "fail_on_quality_violation": True,
            },
            "sla_rules": {
                "max_total_seconds": 900,
                "max_extract_seconds": 600,
                "max_silver_seconds": 240,
                "max_gold_seconds": 240,
                "min_raw_records": 1,
                "require_quality_passed": True,
                "fail_on_sla_violation": True,
            },
            "delta_config": {
                "enabled": False,
                "catalog": "",
                "schema": "weather_dev",
                "silver_table": "openweather_current_weather_delta",
                "gold_table": "weather_city_daily_snapshot_delta",
                "checkpoint_table": "openweather_pipeline_checkpoint",
                "merge_schema": True,
                "create_schema_if_missing": True,
            },
        },
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _build_override_candidates(local_override_file: str) -> list[Path]:
    override_file = (local_override_file or "").strip()
    if not override_file:
        return []

    candidates: list[Path] = [Path(override_file)]
    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        )
        marker = "/notebooks/databricks/"
        if marker in notebook_path and not override_file.startswith("/"):
            repo_root = notebook_path.split(marker, 1)[0]
            candidates.append(Path("/Workspace") / repo_root.lstrip("/") / override_file)
    except Exception:
        pass

    unique_candidates: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key not in seen:
            seen.add(key)
            unique_candidates.append(candidate)
    return unique_candidates


def _load_optional_local_override(local_override_file: str) -> tuple[dict[str, Any], str]:
    for candidate in _build_override_candidates(local_override_file):
        if not candidate.exists():
            continue
        with candidate.open("r", encoding="utf-8") as file_handle:
            payload = json.load(file_handle)
        if not isinstance(payload, dict):
            raise ValueError(
                f"Local override file must contain a JSON object: {candidate}"
            )
        return payload, str(candidate)
    return {}, ""


# COMMAND ----------
try:
    if ACTIVE_PROFILE not in PIPELINE_PROFILES:
        available = ", ".join(sorted(PIPELINE_PROFILES))
        raise ValueError(
            f"Invalid ACTIVE_PROFILE='{ACTIVE_PROFILE}'. Available profiles: {available}."
        )

    base_settings = PIPELINE_PROFILES[ACTIVE_PROFILE]
    override_settings, override_path = _load_optional_local_override(LOCAL_OVERRIDE_FILE)
    pipeline_settings = _deep_merge(base_settings, override_settings)

    config = build_runtime_config_from_manual_input(
        pipeline_settings["manual_config"],
        allow_plaintext_credentials=bool(
            pipeline_settings.get("allow_plaintext_credentials", False)
        ),
    )
    output_payload = run_full_pipeline(
        config,
        stage_name="full_pipeline_no_widgets",
        create_container_if_missing=bool(
            pipeline_settings.get("create_container_if_missing", True)
        ),
    )
    output_payload["active_profile"] = ACTIVE_PROFILE
    output_payload["local_override_file"] = override_path
    stage_success(output_payload)
except Exception as exc:
    stage_error(
        "full_pipeline_no_widgets",
        exc,
        context={
            "active_profile": ACTIVE_PROFILE,
            "local_override_file": LOCAL_OVERRIDE_FILE,
        },
    )
