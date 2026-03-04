# Databricks notebook source
# MAGIC %md
# MAGIC # 98 - Full Pipeline (No Widgets)
# MAGIC
# MAGIC Manual run for first-time validation, without `dbutils.widgets`.
# MAGIC Fill credentials below and run all cells.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
# EDIT ONLY THIS CELL
MANUAL_CONFIG = {
    "storage_account": "tropowxdlprod",
    "container": "openweather-data",
    "openweather_api_key": "<OPENWEATHER_API_KEY>",
    "storage_account_key": "<AZURE_STORAGE_ACCOUNT_KEY>",
    "openweather_base_url": "https://api.openweathermap.org/data/2.5",
    "openweather_endpoints": ["weather"],
    "openweather_units": "metric",
    "openweather_lang": "pt_br",
    "openweather_timeout_seconds": 30,
    "cities": ["Sao Paulo,BR", "Rio de Janeiro,BR", "Curitiba,BR"],
}


# COMMAND ----------
try:
    if (
        MANUAL_CONFIG["openweather_api_key"].startswith("<")
        or MANUAL_CONFIG["storage_account_key"].startswith("<")
    ):
        raise ValueError(
            "Preencha openweather_api_key e storage_account_key na celula MANUAL_CONFIG."
        )

    config = {
        "storage_account": MANUAL_CONFIG["storage_account"],
        "container": MANUAL_CONFIG["container"],
        "storage_account_url": (
            f"https://{MANUAL_CONFIG['storage_account']}.blob.core.windows.net"
        ),
        "storage_auth": {
            "auth_type": "credential",
            "credential": MANUAL_CONFIG["storage_account_key"],
            "credential_kind": "account_key",
            "source": "manual:notebook",
        },
        "openweather_base_url": MANUAL_CONFIG["openweather_base_url"],
        "openweather_endpoints": MANUAL_CONFIG["openweather_endpoints"],
        "openweather_units": MANUAL_CONFIG["openweather_units"],
        "openweather_lang": MANUAL_CONFIG["openweather_lang"],
        "openweather_timeout_seconds": int(MANUAL_CONFIG["openweather_timeout_seconds"]),
        "cities": MANUAL_CONFIG["cities"],
        "openweather_api_key": MANUAL_CONFIG["openweather_api_key"],
        "openweather_api_key_source": "manual:notebook",
    }

    container_client = get_container_client(config, create_if_missing=True)
    watermark = build_watermark()
    preflight_path = storage_preflight_check(container_client, config, watermark)

    raw_records = 0
    bronze_records = 0
    bronze_records_cache = []

    for endpoint in config["openweather_endpoints"]:
        for city in config["cities"]:
            response = fetch_openweather_payload(config, endpoint, city)
            payload = response["payload"]

            raw_record = build_raw_record(
                endpoint=endpoint,
                city_query=city,
                status_code=response["status_code"],
                request_url=response["request_url"],
                payload=payload,
                watermark=watermark,
            )
            raw_path = build_record_blob_path(
                layer="raw",
                endpoint=endpoint,
                city_query=city,
                watermark=watermark,
                extension="json",
            )
            upload_json_blob(
                container_client,
                raw_path,
                raw_record,
                compact=False,
                metadata=build_blob_metadata("raw", watermark),
            )
            raw_records += 1

            bronze_record = build_bronze_record(
                endpoint=endpoint,
                city_query=city,
                payload=payload,
                watermark=watermark,
            )
            bronze_path = build_record_blob_path(
                layer="bronze",
                endpoint=endpoint,
                city_query=city,
                watermark=watermark,
                extension="json",
            )
            upload_json_blob(
                container_client,
                bronze_path,
                bronze_record,
                compact=True,
                metadata=build_blob_metadata("bronze", watermark),
            )
            bronze_records += 1
            bronze_records_cache.append(bronze_record)

    if not bronze_records_cache:
        raise ValueError("No bronze records produced; cannot proceed to silver/gold.")

    silver_rows = [bronze_to_silver_row(record) for record in bronze_records_cache]
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

    silver_blob_path = build_dataset_blob_path(
        layer="silver",
        dataset="openweather_current_weather",
        run_id=watermark["run_id"],
        ingestion_date=watermark["ingestion_date"],
        ingestion_hour=watermark["ingestion_hour"],
        extension="parquet",
    )
    upload_parquet_blob(
        container_client,
        silver_blob_path,
        silver_df,
        metadata=build_blob_metadata("silver", watermark),
    )

    source_max_epoch = pd.to_numeric(silver_df["observation_epoch"], errors="coerce").max()
    source_max_epoch = int(source_max_epoch) if pd.notna(source_max_epoch) else None

    gold_df = (
        silver_df.groupby(["event_date", "city_name", "country"], dropna=False)
        .agg(
            records_count=("city_id", "count"),
            temperature_avg_celsius=("temperature_celsius", "mean"),
            temperature_min_celsius=("temp_min_celsius", "min"),
            temperature_max_celsius=("temp_max_celsius", "max"),
            humidity_avg_pct=("humidity_pct", "mean"),
            wind_speed_avg_ms=("wind_speed_ms", "mean"),
            last_observation_ts_utc=("observation_ts_utc", "max"),
        )
        .reset_index()
    )
    gold_df["watermark_run_id"] = watermark["run_id"]
    gold_df["watermark_ingestion_ts_utc"] = watermark["ingestion_ts_utc"]
    gold_df["watermark_ingestion_epoch"] = watermark["ingestion_epoch"]
    gold_df["watermark_source_max_epoch"] = source_max_epoch
    gold_df["watermark_source_max_ts_utc"] = epoch_to_iso_utc(source_max_epoch)

    gold_blob_path = build_dataset_blob_path(
        layer="gold",
        dataset="weather_city_daily_snapshot",
        run_id=watermark["run_id"],
        ingestion_date=watermark["ingestion_date"],
        ingestion_hour=watermark["ingestion_hour"],
        extension="parquet",
    )
    upload_parquet_blob(
        container_client,
        gold_blob_path,
        gold_df,
        metadata=build_blob_metadata("gold", watermark),
    )

    manifest = {
        "run_id": watermark["run_id"],
        "ingestion_ts_utc": watermark["ingestion_ts_utc"],
        "ingestion_epoch": watermark["ingestion_epoch"],
        "ingestion_date": watermark["ingestion_date"],
        "ingestion_hour": watermark["ingestion_hour"],
        "container": config["container"],
        "storage_account": config["storage_account"],
        "cities": config["cities"],
        "openweather_endpoints": config["openweather_endpoints"],
        "openweather_api_key_source": config["openweather_api_key_source"],
        "storage_auth_source": config["storage_auth"]["source"],
        "raw_records": raw_records,
        "bronze_records": bronze_records,
        "silver_rows": len(silver_df),
        "gold_rows": len(gold_df),
        "storage_preflight_path": preflight_path,
    }
    manifest_path = write_run_manifest(container_client, manifest)

    output_payload = {
        "status": "ok",
        "stage": "full_pipeline_no_widgets",
        "run_id": watermark["run_id"],
        "ingestion_date": watermark["ingestion_date"],
        "ingestion_hour": watermark["ingestion_hour"],
        "raw_records": raw_records,
        "bronze_records": bronze_records,
        "silver_rows": len(silver_df),
        "gold_rows": len(gold_df),
        "storage_preflight_path": preflight_path,
        "silver_blob_path": silver_blob_path,
        "gold_blob_path": gold_blob_path,
        "manifest_path": manifest_path,
    }
    stage_success(output_payload)
except Exception as exc:
    stage_error("full_pipeline_no_widgets", exc)
