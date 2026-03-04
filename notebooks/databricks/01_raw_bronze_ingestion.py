# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Raw + Bronze Ingestion
# MAGIC
# MAGIC Serverless-safe ingestion:
# MAGIC - calls OpenWeather API
# MAGIC - writes raw and bronze JSON directly to Azure Blob via SDK
# MAGIC - writes run manifest for downstream stages

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
try:
    config = get_runtime_config(require_api_key=True)
    container_client = get_container_client(config, create_if_missing=True)
    watermark = build_watermark()
    preflight_path = storage_preflight_check(container_client, config, watermark)

    raw_records = 0
    bronze_records = 0

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
        "storage_preflight_path": preflight_path,
    }
    manifest_path = write_run_manifest(container_client, manifest)

    output_payload = {
        "status": "ok",
        "stage": "raw_bronze",
        "run_id": watermark["run_id"],
        "ingestion_date": watermark["ingestion_date"],
        "ingestion_hour": watermark["ingestion_hour"],
        "raw_records": raw_records,
        "bronze_records": bronze_records,
        "openweather_api_key_source": config["openweather_api_key_source"],
        "storage_auth_source": config["storage_auth"]["source"],
        "storage_preflight_path": preflight_path,
        "manifest_path": manifest_path,
    }
    stage_success(output_payload)
except Exception as exc:
    stage_error("raw_bronze", exc)
