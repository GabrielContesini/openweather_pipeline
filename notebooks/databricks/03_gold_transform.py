# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Transform
# MAGIC
# MAGIC Serverless-safe transform:
# MAGIC - reads one silver parquet blob
# MAGIC - writes gold aggregate parquet blob

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
ensure_base_widgets()

# COMMAND ----------
ensure_widget("p_run_id", "")
ensure_widget("p_ingestion_date", "")
ensure_widget("p_ingestion_hour", "")

try:
    config = get_runtime_config(require_api_key=False)
    container_client = get_container_client(config, create_if_missing=False)

    run_context = resolve_run_context(
        container_client,
        run_id=dbutils.widgets.get("p_run_id").strip(),
        ingestion_date=dbutils.widgets.get("p_ingestion_date").strip(),
        ingestion_hour=dbutils.widgets.get("p_ingestion_hour").strip(),
    )

    silver_blob_path = build_dataset_blob_path(
        layer="silver",
        dataset="openweather_current_weather",
        run_id=run_context["run_id"],
        ingestion_date=run_context["ingestion_date"],
        ingestion_hour=run_context["ingestion_hour"],
        extension="parquet",
    )
    silver_df = download_parquet_blob(container_client, silver_blob_path)
    if silver_df.empty:
        raise ValueError("Silver dataframe is empty. Gold cannot be generated.")

    source_max_epoch = pd.to_numeric(silver_df["observation_epoch"], errors="coerce").max()
    source_max_epoch = int(source_max_epoch) if pd.notna(source_max_epoch) else None
    source_max_ts_utc = epoch_to_iso_utc(source_max_epoch)

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

    watermark_ingestion_ts_utc = str(silver_df.iloc[0]["watermark_ingestion_ts_utc"])
    watermark_ingestion_epoch = int(float(silver_df.iloc[0]["watermark_ingestion_epoch"]))
    gold_df["watermark_run_id"] = run_context["run_id"]
    gold_df["watermark_ingestion_ts_utc"] = watermark_ingestion_ts_utc
    gold_df["watermark_ingestion_epoch"] = watermark_ingestion_epoch
    gold_df["watermark_source_max_epoch"] = source_max_epoch
    gold_df["watermark_source_max_ts_utc"] = source_max_ts_utc

    gold_blob_path = build_dataset_blob_path(
        layer="gold",
        dataset="weather_city_daily_snapshot",
        run_id=run_context["run_id"],
        ingestion_date=run_context["ingestion_date"],
        ingestion_hour=run_context["ingestion_hour"],
        extension="parquet",
    )
    gold_watermark = {
        "run_id": run_context["run_id"],
        "ingestion_ts_utc": watermark_ingestion_ts_utc,
        "ingestion_epoch": watermark_ingestion_epoch,
    }
    upload_parquet_blob(
        container_client,
        gold_blob_path,
        gold_df,
        metadata=build_blob_metadata("gold", gold_watermark),
    )

    output_payload = {
        "status": "ok",
        "stage": "gold",
        "run_id": run_context["run_id"],
        "ingestion_date": run_context["ingestion_date"],
        "ingestion_hour": run_context["ingestion_hour"],
        "input_blob_path": silver_blob_path,
        "output_blob_path": gold_blob_path,
        "row_count": len(gold_df),
        "run_context_source": run_context["source"],
    }
    stage_success(output_payload)
except Exception as exc:
    stage_error("gold", exc)
