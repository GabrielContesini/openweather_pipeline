# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transform
# MAGIC
# MAGIC Serverless-safe transform:
# MAGIC - reads bronze JSON directly from Azure Blob
# MAGIC - builds curated silver table in Parquet

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
ensure_base_widgets()

# COMMAND ----------
ensure_widget("p_run_id", "")
ensure_widget("p_ingestion_date", "")
ensure_widget("p_ingestion_hour", "")
ensure_widget("p_endpoint_for_silver", "weather")

try:
    config = get_runtime_config(require_api_key=False)
    container_client = get_container_client(config, create_if_missing=False)

    run_context = resolve_run_context(
        container_client,
        run_id=dbutils.widgets.get("p_run_id").strip(),
        ingestion_date=dbutils.widgets.get("p_ingestion_date").strip(),
        ingestion_hour=dbutils.widgets.get("p_ingestion_hour").strip(),
    )
    endpoint = dbutils.widgets.get("p_endpoint_for_silver").strip() or "weather"

    bronze_prefix = (
        f"bronze/openweather/{endpoint}/"
        f"ingestion_date={run_context['ingestion_date']}/"
        f"ingestion_hour={run_context['ingestion_hour']}/"
    )
    bronze_blob_names = [
        blob_name
        for blob_name in list_blob_names(container_client, bronze_prefix)
        if f"run_id={run_context['run_id']}.json" in blob_name
    ]
    if not bronze_blob_names:
        raise ValueError(
            f"No bronze blobs found for run_id={run_context['run_id']} under prefix {bronze_prefix}"
        )

    silver_rows = [
        bronze_to_silver_row(download_json_blob(container_client, blob_name))
        for blob_name in bronze_blob_names
    ]
    silver_df = pd.DataFrame(silver_rows)
    if silver_df.empty:
        raise ValueError("Silver dataframe is empty after bronze transformation.")

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
        run_id=run_context["run_id"],
        ingestion_date=run_context["ingestion_date"],
        ingestion_hour=run_context["ingestion_hour"],
        extension="parquet",
    )
    silver_watermark = {
        "run_id": run_context["run_id"],
        "ingestion_ts_utc": str(silver_df.iloc[0]["watermark_ingestion_ts_utc"]),
        "ingestion_epoch": int(float(silver_df.iloc[0]["watermark_ingestion_epoch"])),
    }
    upload_parquet_blob(
        container_client,
        silver_blob_path,
        silver_df,
        metadata=build_blob_metadata("silver", silver_watermark),
    )

    output_payload = {
        "status": "ok",
        "stage": "silver",
        "run_id": run_context["run_id"],
        "ingestion_date": run_context["ingestion_date"],
        "ingestion_hour": run_context["ingestion_hour"],
        "input_prefix": bronze_prefix,
        "input_blob_count": len(bronze_blob_names),
        "output_blob_path": silver_blob_path,
        "row_count": len(silver_df),
        "run_context_source": run_context["source"],
    }
    stage_success(output_payload)
except Exception as exc:
    stage_error("silver", exc)
