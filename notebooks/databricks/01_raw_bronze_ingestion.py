# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Raw + Bronze Ingestion
# MAGIC
# MAGIC Extracts OpenWeather API data, writes `raw` and `bronze` layers to ADLS Gen2, and registers a run manifest.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
config = get_runtime_config()
configure_storage_access(config["storage_account"], config["storage_account_key"])
watermark = build_watermark()

raw_records = 0
bronze_records = 0

for endpoint in config["openweather_endpoints"]:
    for city in config["cities"]:
        city_slug = slugify_city(city)
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
        raw_path = build_record_path(
            base_uri=config["base_uri"],
            layer="raw",
            endpoint=endpoint,
            city_slug=city_slug,
            watermark=watermark,
            extension="json",
        )
        write_json_document(raw_path, raw_record, compact=False)
        raw_records += 1

        bronze_record = build_bronze_record(
            endpoint=endpoint,
            city_query=city,
            payload=payload,
            watermark=watermark,
        )
        bronze_path = build_record_path(
            base_uri=config["base_uri"],
            layer="bronze",
            endpoint=endpoint,
            city_slug=city_slug,
            watermark=watermark,
            extension="json",
        )
        write_json_document(bronze_path, bronze_record, compact=True)
        bronze_records += 1

manifest = {
    "run_id": watermark["run_id"],
    "ingestion_ts_utc": watermark["ingestion_ts_utc"],
    "ingestion_epoch": watermark["ingestion_epoch"],
    "ingestion_date": watermark["ingestion_date"],
    "ingestion_hour": watermark["ingestion_hour"],
    "base_uri": config["base_uri"],
    "container": config["container"],
    "storage_account": config["storage_account"],
    "cities": config["cities"],
    "openweather_endpoints": config["openweather_endpoints"],
    "raw_records": raw_records,
    "bronze_records": bronze_records,
}
manifest_path = write_run_manifest(config["base_uri"], manifest)

output_payload = {
    "status": "ok",
    "stage": "raw_bronze",
    "run_id": watermark["run_id"],
    "ingestion_date": watermark["ingestion_date"],
    "ingestion_hour": watermark["ingestion_hour"],
    "raw_records": raw_records,
    "bronze_records": bronze_records,
    "manifest_path": manifest_path,
}

print(json.dumps(output_payload, indent=2))
dbutils.notebook.exit(json.dumps(output_payload))
