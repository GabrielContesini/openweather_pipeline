# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Smoke Test
# MAGIC
# MAGIC Validates dependencies, credentials, storage write, and one OpenWeather call.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
config = get_runtime_config(require_api_key=True)
container_client = get_container_client(config, create_if_missing=True)
watermark = build_watermark()
preflight_path = storage_preflight_check(container_client, config, watermark)

endpoint = config["openweather_endpoints"][0]
city = config["cities"][0]
api_response = fetch_openweather_payload(config, endpoint, city)

output_payload = {
    "status": "ok",
    "stage": "smoke_test",
    "storage_account": config["storage_account"],
    "container": config["container"],
    "storage_auth_source": config["storage_auth"]["source"],
    "openweather_api_key_source": config["openweather_api_key_source"],
    "preflight_path": preflight_path,
    "openweather_endpoint": endpoint,
    "openweather_city": city,
    "openweather_status_code": api_response["status_code"],
}

print(json.dumps(output_payload, indent=2, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(output_payload))
