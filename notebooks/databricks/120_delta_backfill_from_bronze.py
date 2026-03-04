# Databricks notebook source
# MAGIC %md
# MAGIC # 120 - Delta Backfill from Bronze
# MAGIC
# MAGIC Rebuilds incremental Delta layers from bronze blobs for a date range.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any


ACTIVE_PROFILE = "free_plaintext_local"
LOCAL_OVERRIDE_FILE = "config/databricks_free.local.json"

PIPELINE_PROFILES = {
    "prod_secret_scope": {
        "allow_plaintext_credentials": False,
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
            "delta_config": {
                "enabled": True,
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
            "delta_config": {
                "enabled": True,
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

BACKFILL_SETTINGS = {
    "start_date": "2026-03-01",
    "end_date": "2026-03-31",
    "endpoint": "weather",
    "run_ids": [],
    "max_runs": 1000,
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


def parse_iso_date(date_text: str) -> datetime:
    return datetime.strptime(date_text, "%Y-%m-%d")


def list_run_manifests_in_range(
    container_client: Any,
    *,
    start_date: str,
    end_date: str,
    run_ids: set[str],
    max_runs: int,
) -> list[dict[str, Any]]:
    start_dt = parse_iso_date(start_date)
    end_dt = parse_iso_date(end_date)
    if start_dt > end_dt:
        raise ValueError("BACKFILL_SETTINGS.start_date must be <= end_date.")

    manifest_prefix = "raw/openweather/_control/runs/"
    selected_manifests: list[dict[str, Any]] = []
    for blob in container_client.list_blobs(name_starts_with=manifest_prefix):
        manifest = download_json_blob(container_client, blob.name)
        manifest_date = manifest.get("ingestion_date", "")
        if not manifest_date:
            continue

        manifest_dt = parse_iso_date(manifest_date)
        if manifest_dt < start_dt or manifest_dt > end_dt:
            continue

        run_id = str(manifest.get("run_id", "")).strip()
        if run_ids and run_id not in run_ids:
            continue

        selected_manifests.append(manifest)

    selected_manifests.sort(
        key=lambda item: (int(item.get("ingestion_epoch", 0)), str(item.get("run_id", "")))
    )
    if max_runs > 0:
        selected_manifests = selected_manifests[:max_runs]
    return selected_manifests


def load_silver_rows_from_bronze(
    container_client: Any,
    *,
    manifests: list[dict[str, Any]],
    endpoint: str,
) -> tuple[list[dict[str, Any]], int]:
    silver_rows: list[dict[str, Any]] = []
    bronze_blob_count = 0

    for manifest in manifests:
        run_id = manifest["run_id"]
        ingestion_date = manifest["ingestion_date"]
        ingestion_hour = manifest["ingestion_hour"]
        bronze_prefix = (
            f"bronze/openweather/{endpoint}/"
            f"ingestion_date={ingestion_date}/"
            f"ingestion_hour={ingestion_hour}/"
        )
        bronze_blob_names = [
            blob_name
            for blob_name in list_blob_names(container_client, bronze_prefix)
            if f"run_id={run_id}.json" in blob_name
        ]

        for blob_name in bronze_blob_names:
            bronze_record = download_json_blob(container_client, blob_name)
            silver_rows.append(bronze_to_silver_row(bronze_record))
        bronze_blob_count += len(bronze_blob_names)

    return silver_rows, bronze_blob_count


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

    if not bool(config["delta_config"]["enabled"]):
        raise ValueError("delta_config.enabled must be true for backfill notebook.")

    extraction_started = time.perf_counter()
    container_client = get_container_client(config, create_if_missing=False)
    run_ids_filter = {str(run_id).strip() for run_id in BACKFILL_SETTINGS.get("run_ids", []) if str(run_id).strip()}
    manifests = list_run_manifests_in_range(
        container_client,
        start_date=BACKFILL_SETTINGS["start_date"],
        end_date=BACKFILL_SETTINGS["end_date"],
        run_ids=run_ids_filter,
        max_runs=int(BACKFILL_SETTINGS.get("max_runs", 1000)),
    )
    if not manifests:
        raise ValueError("No manifests found for the requested backfill range.")

    silver_rows, bronze_blob_count = load_silver_rows_from_bronze(
        container_client,
        manifests=manifests,
        endpoint=BACKFILL_SETTINGS.get("endpoint", "weather"),
    )
    if not silver_rows:
        raise ValueError("No bronze rows found for selected manifests.")

    extract_seconds = round(time.perf_counter() - extraction_started, 3)

    silver_started = time.perf_counter()
    silver_df = pd.DataFrame(silver_rows)
    silver_df = coerce_numeric_columns(
        silver_df,
        [
            "watermark_ingestion_epoch",
            "watermark_source_epoch",
            "city_id",
            "latitude",
            "longitude",
            "observation_epoch",
            "timezone_offset_seconds",
            "temperature_celsius",
            "feels_like_celsius",
            "temp_min_celsius",
            "temp_max_celsius",
            "pressure_hpa",
            "humidity_pct",
            "wind_speed_ms",
            "wind_deg",
            "cloudiness_pct",
        ],
    )
    silver_seconds = round(time.perf_counter() - silver_started, 3)

    gold_started = time.perf_counter()
    watermark = build_watermark()
    gold_df = build_gold_dataframe(silver_df, watermark)
    gold_seconds = round(time.perf_counter() - gold_started, 3)

    delta_started = time.perf_counter()
    delta_report = run_delta_upserts(
        config=config,
        silver_df=silver_df,
        gold_df=gold_df,
        watermark=watermark,
        timings={
            "extract_seconds": extract_seconds,
            "silver_seconds": silver_seconds,
            "gold_seconds": gold_seconds,
        },
        raw_records=bronze_blob_count,
    )
    delta_seconds = round(time.perf_counter() - delta_started, 3)
    total_seconds = round(
        extract_seconds + silver_seconds + gold_seconds + delta_seconds,
        3,
    )

    output_payload = {
        "status": "ok",
        "stage": "delta_backfill_from_bronze",
        "active_profile": ACTIVE_PROFILE,
        "local_override_file": override_path,
        "backfill_start_date": BACKFILL_SETTINGS["start_date"],
        "backfill_end_date": BACKFILL_SETTINGS["end_date"],
        "selected_runs": len(manifests),
        "bronze_blobs_processed": bronze_blob_count,
        "silver_rows": len(silver_df),
        "gold_rows": len(gold_df),
        "delta_report": delta_report,
        "timings": {
            "extract_seconds": extract_seconds,
            "silver_seconds": silver_seconds,
            "gold_seconds": gold_seconds,
            "delta_seconds": delta_seconds,
            "total_seconds": total_seconds,
        },
    }
    stage_success(output_payload)
except Exception as exc:
    stage_error(
        "delta_backfill_from_bronze",
        exc,
        context={
            "active_profile": ACTIVE_PROFILE,
            "local_override_file": LOCAL_OVERRIDE_FILE,
            "backfill_settings": BACKFILL_SETTINGS,
        },
    )
