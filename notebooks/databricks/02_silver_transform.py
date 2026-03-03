# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transform
# MAGIC
# MAGIC Reads `bronze` weather files for one run and writes curated `silver` Delta dataset.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
ensure_widget("p_run_id", "")
ensure_widget("p_ingestion_date", "")
ensure_widget("p_ingestion_hour", "")
ensure_widget("p_endpoint_for_silver", "weather")

config = get_runtime_config(require_api_key=False)
configure_storage_access(config["storage_account"], config["storage_account_key"])

run_context = resolve_run_context(
    base_uri=config["base_uri"],
    run_id=dbutils.widgets.get("p_run_id").strip(),
    ingestion_date=dbutils.widgets.get("p_ingestion_date").strip(),
    ingestion_hour=dbutils.widgets.get("p_ingestion_hour").strip(),
)
endpoint = dbutils.widgets.get("p_endpoint_for_silver").strip() or "weather"

bronze_path = (
    f"{config['base_uri']}/bronze/openweather/{endpoint}/"
    f"ingestion_date={run_context['ingestion_date']}/"
    f"ingestion_hour={run_context['ingestion_hour']}/"
)

bronze_df = spark.read.option("multiLine", "true").json(bronze_path)

silver_df = (
    bronze_df.withColumn("event_date", F.to_date("observation_ts_utc"))
    .withColumn("watermark_ingestion_epoch", F.col("watermark_ingestion_epoch").cast("long"))
    .withColumn("watermark_source_epoch", F.col("watermark_source_epoch").cast("long"))
    .withColumn("observation_epoch", F.col("observation_epoch").cast("long"))
    .withColumn("city_id", F.col("city_id").cast("long"))
    .withColumn("temperature_celsius", F.col("temperature_celsius").cast("double"))
    .withColumn("feels_like_celsius", F.col("feels_like_celsius").cast("double"))
    .withColumn("temp_min_celsius", F.col("temp_min_celsius").cast("double"))
    .withColumn("temp_max_celsius", F.col("temp_max_celsius").cast("double"))
    .withColumn("pressure_hpa", F.col("pressure_hpa").cast("double"))
    .withColumn("humidity_pct", F.col("humidity_pct").cast("double"))
    .withColumn("wind_speed_ms", F.col("wind_speed_ms").cast("double"))
    .withColumn("wind_deg", F.col("wind_deg").cast("double"))
    .withColumn("cloudiness_pct", F.col("cloudiness_pct").cast("double"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .select(
        "watermark_run_id",
        "watermark_ingestion_ts_utc",
        "watermark_ingestion_epoch",
        "watermark_source_epoch",
        "watermark_source_ts_utc",
        "event_date",
        "city_id",
        "city_name",
        "country",
        "latitude",
        "longitude",
        "observation_epoch",
        "observation_ts_utc",
        "timezone_offset_seconds",
        "weather_main",
        "weather_description",
        "temperature_celsius",
        "feels_like_celsius",
        "temp_min_celsius",
        "temp_max_celsius",
        "pressure_hpa",
        "humidity_pct",
        "wind_speed_ms",
        "wind_deg",
        "cloudiness_pct",
        "sunrise_ts_utc",
        "sunset_ts_utc",
    )
)

silver_row_count = silver_df.count()
silver_path = (
    f"{config['base_uri']}/silver/openweather/openweather_current_weather/"
    f"ingestion_date={run_context['ingestion_date']}/"
    f"ingestion_hour={run_context['ingestion_hour']}/"
    f"run_id={run_context['run_id']}"
)

silver_df.write.format("delta").mode("overwrite").save(silver_path)

output_payload = {
    "status": "ok",
    "stage": "silver",
    "run_id": run_context["run_id"],
    "ingestion_date": run_context["ingestion_date"],
    "ingestion_hour": run_context["ingestion_hour"],
    "input_path": bronze_path,
    "output_path": silver_path,
    "row_count": silver_row_count,
    "run_context_source": run_context["source"],
}

print(json.dumps(output_payload, indent=2))
dbutils.notebook.exit(json.dumps(output_payload))
