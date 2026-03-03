# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Transform
# MAGIC
# MAGIC Reads one silver run and writes daily city aggregates to `gold` Delta dataset.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
ensure_widget("p_run_id", "")
ensure_widget("p_ingestion_date", "")
ensure_widget("p_ingestion_hour", "")

config = get_runtime_config(require_api_key=False)
configure_storage_access(config["storage_account"], config["storage_account_key"])

run_context = resolve_run_context(
    base_uri=config["base_uri"],
    run_id=dbutils.widgets.get("p_run_id").strip(),
    ingestion_date=dbutils.widgets.get("p_ingestion_date").strip(),
    ingestion_hour=dbutils.widgets.get("p_ingestion_hour").strip(),
)

silver_path = (
    f"{config['base_uri']}/silver/openweather/openweather_current_weather/"
    f"ingestion_date={run_context['ingestion_date']}/"
    f"ingestion_hour={run_context['ingestion_hour']}/"
    f"run_id={run_context['run_id']}"
)
silver_df = spark.read.format("delta").load(silver_path)

watermark_row = silver_df.select(
    "watermark_ingestion_ts_utc", "watermark_ingestion_epoch"
).first()
silver_ingestion_ts_utc = (
    watermark_row["watermark_ingestion_ts_utc"] if watermark_row else None
)
silver_ingestion_epoch = (
    int(watermark_row["watermark_ingestion_epoch"]) if watermark_row else None
)

source_max_epoch = silver_df.agg(F.max("observation_epoch").alias("max_epoch")).first()[
    "max_epoch"
]
source_max_ts_utc = (
    datetime.fromtimestamp(int(source_max_epoch), timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
    if source_max_epoch is not None
    else None
)

gold_df = (
    silver_df.groupBy("event_date", "city_name", "country")
    .agg(
        F.count("*").alias("records_count"),
        F.avg("temperature_celsius").alias("temperature_avg_celsius"),
        F.min("temp_min_celsius").alias("temperature_min_celsius"),
        F.max("temp_max_celsius").alias("temperature_max_celsius"),
        F.avg("humidity_pct").alias("humidity_avg_pct"),
        F.avg("wind_speed_ms").alias("wind_speed_avg_ms"),
        F.max("observation_ts_utc").alias("last_observation_ts_utc"),
    )
    .withColumn("watermark_run_id", F.lit(run_context["run_id"]))
    .withColumn("watermark_ingestion_ts_utc", F.lit(silver_ingestion_ts_utc))
    .withColumn("watermark_ingestion_epoch", F.lit(silver_ingestion_epoch).cast("long"))
    .withColumn("watermark_source_max_epoch", F.lit(source_max_epoch).cast("long"))
    .withColumn("watermark_source_max_ts_utc", F.lit(source_max_ts_utc))
)

gold_row_count = gold_df.count()
gold_path = (
    f"{config['base_uri']}/gold/openweather/weather_city_daily_snapshot/"
    f"ingestion_date={run_context['ingestion_date']}/"
    f"ingestion_hour={run_context['ingestion_hour']}/"
    f"run_id={run_context['run_id']}"
)
gold_df.write.format("delta").mode("overwrite").save(gold_path)

output_payload = {
    "status": "ok",
    "stage": "gold",
    "run_id": run_context["run_id"],
    "ingestion_date": run_context["ingestion_date"],
    "ingestion_hour": run_context["ingestion_hour"],
    "input_path": silver_path,
    "output_path": gold_path,
    "row_count": gold_row_count,
    "run_context_source": run_context["source"],
}

print(json.dumps(output_payload, indent=2))
dbutils.notebook.exit(json.dumps(output_payload))
