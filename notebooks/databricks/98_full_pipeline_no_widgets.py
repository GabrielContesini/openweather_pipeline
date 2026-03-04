# Databricks notebook source
# MAGIC %md
# MAGIC # 98 - Full Pipeline (No Widgets, Production Path)
# MAGIC
# MAGIC Official pipeline entrypoint for Databricks Jobs and manual execution.
# MAGIC
# MAGIC Credential options in this notebook:
# MAGIC 1. `secret://<scope>/<key>` (recommended)
# MAGIC 2. Plaintext (only if `allow_plaintext_credentials=True`)

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
# EDIT ONLY THIS CELL
PIPELINE_SETTINGS = {
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
    },
}


# COMMAND ----------
try:
    config = build_runtime_config_from_manual_input(
        PIPELINE_SETTINGS["manual_config"],
        allow_plaintext_credentials=bool(
            PIPELINE_SETTINGS.get("allow_plaintext_credentials", False)
        ),
    )
    output_payload = run_full_pipeline(
        config,
        stage_name="full_pipeline_no_widgets",
        create_container_if_missing=bool(
            PIPELINE_SETTINGS.get("create_container_if_missing", True)
        ),
    )
    stage_success(output_payload)
except Exception as exc:
    stage_error("full_pipeline_no_widgets", exc)
