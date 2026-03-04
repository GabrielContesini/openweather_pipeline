# Databricks notebook source
# MAGIC %md
# MAGIC # 110 - Unity Catalog Governance Bootstrap
# MAGIC
# MAGIC Creates UC catalog/schema objects, base Delta tables, views, and ACL grants.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
UC_SETTINGS = {
    "catalog": "weather_prd",
    "ops_schema": "weather_ops",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "gold",
    "monitoring_schema": "monitoring",
    "silver_delta_table": "openweather_current_weather_delta",
    "gold_delta_table": "weather_city_daily_snapshot_delta",
    "checkpoint_delta_table": "openweather_pipeline_checkpoint",
    "apply_grants": True,
    "principals": {
        "data_engineers": "data_engineers",
        "data_analysts": "data_analysts",
        "data_scientists": "data_scientists",
    },
}


def q(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def fqn(catalog: str, schema: str, object_name: str | None = None) -> str:
    if object_name:
        return f"{q(catalog)}.{q(schema)}.{q(object_name)}"
    return f"{q(catalog)}.{q(schema)}"


def safe_execute(sql_text: str, *, errors: list[str]) -> None:
    try:
        spark.sql(sql_text)
    except Exception as exc:
        errors.append(f"{sql_text} -> {type(exc).__name__}: {exc}")


def is_uc_unavailable_error(exc: Exception) -> bool:
    message = str(exc).lower()
    patterns = [
        "unity catalog",
        "create catalog",
        "insufficient privileges",
        "permission denied",
        "unsupported",
        "catalog is not enabled",
        "feature is not available",
    ]
    return any(pattern in message for pattern in patterns)


errors: list[str] = []
warnings: list[str] = []

try:
    catalog = UC_SETTINGS["catalog"]
    ops_schema = UC_SETTINGS["ops_schema"]
    bronze_schema = UC_SETTINGS["bronze_schema"]
    silver_schema = UC_SETTINGS["silver_schema"]
    gold_schema = UC_SETTINGS["gold_schema"]
    monitoring_schema = UC_SETTINGS["monitoring_schema"]

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {q(catalog)}")
    for schema_name in [
        ops_schema,
        bronze_schema,
        silver_schema,
        gold_schema,
        monitoring_schema,
    ]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn(catalog, schema_name)}")

    silver_table_fqn = fqn(catalog, ops_schema, UC_SETTINGS["silver_delta_table"])
    gold_table_fqn = fqn(catalog, ops_schema, UC_SETTINGS["gold_delta_table"])
    checkpoint_table_fqn = fqn(catalog, ops_schema, UC_SETTINGS["checkpoint_delta_table"])

    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {silver_table_fqn} (
  city_id BIGINT,
  observation_epoch BIGINT,
  city_name STRING,
  country STRING,
  event_date DATE,
  observation_ts_utc STRING,
  temperature_celsius DOUBLE,
  humidity_pct DOUBLE,
  weather_main STRING,
  weather_description STRING,
  watermark_run_id STRING,
  watermark_ingestion_ts_utc STRING,
  watermark_ingestion_epoch BIGINT,
  pipeline_run_id STRING,
  pipeline_ingestion_ts_utc STRING,
  pipeline_ingestion_date STRING,
  pipeline_ingestion_hour STRING
) USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
"""
    )

    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {gold_table_fqn} (
  event_date DATE,
  city_name STRING,
  country STRING,
  records_count BIGINT,
  temperature_avg_celsius DOUBLE,
  temperature_min_celsius DOUBLE,
  temperature_max_celsius DOUBLE,
  humidity_avg_pct DOUBLE,
  wind_speed_avg_ms DOUBLE,
  last_observation_ts_utc STRING,
  watermark_run_id STRING,
  watermark_ingestion_ts_utc STRING,
  watermark_ingestion_epoch BIGINT,
  pipeline_run_id STRING,
  pipeline_ingestion_ts_utc STRING,
  pipeline_ingestion_date STRING,
  pipeline_ingestion_hour STRING
) USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
"""
    )

    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {checkpoint_table_fqn} (
  run_id STRING,
  ingestion_ts_utc STRING,
  ingestion_epoch BIGINT,
  ingestion_date STRING,
  ingestion_hour STRING,
  raw_records BIGINT,
  silver_rows BIGINT,
  gold_rows BIGINT,
  extract_seconds DOUBLE,
  silver_seconds DOUBLE,
  gold_seconds DOUBLE,
  total_seconds DOUBLE
) USING DELTA
"""
    )

    spark.sql(
        f"""
CREATE OR REPLACE VIEW {fqn(catalog, silver_schema, "vw_openweather_current_weather")} AS
SELECT * FROM {silver_table_fqn}
"""
    )
    spark.sql(
        f"""
CREATE OR REPLACE VIEW {fqn(catalog, gold_schema, "vw_weather_city_daily_snapshot")} AS
SELECT * FROM {gold_table_fqn}
"""
    )

    if UC_SETTINGS["apply_grants"]:
        principals = UC_SETTINGS["principals"]
        engineers = principals["data_engineers"]
        analysts = principals["data_analysts"]
        scientists = principals["data_scientists"]

        grant_statements = [
            f"GRANT USE CATALOG ON CATALOG {q(catalog)} TO `{engineers}`",
            f"GRANT USE CATALOG ON CATALOG {q(catalog)} TO `{analysts}`",
            f"GRANT USE CATALOG ON CATALOG {q(catalog)} TO `{scientists}`",
            f"GRANT USE SCHEMA ON SCHEMA {fqn(catalog, ops_schema)} TO `{engineers}`",
            f"GRANT SELECT, MODIFY ON TABLE {silver_table_fqn} TO `{engineers}`",
            f"GRANT SELECT, MODIFY ON TABLE {gold_table_fqn} TO `{engineers}`",
            f"GRANT SELECT, MODIFY ON TABLE {checkpoint_table_fqn} TO `{engineers}`",
            f"GRANT USE SCHEMA ON SCHEMA {fqn(catalog, silver_schema)} TO `{scientists}`",
            f"GRANT USE SCHEMA ON SCHEMA {fqn(catalog, gold_schema)} TO `{scientists}`",
            f"GRANT SELECT ON VIEW {fqn(catalog, silver_schema, 'vw_openweather_current_weather')} TO `{scientists}`",
            f"GRANT SELECT ON VIEW {fqn(catalog, gold_schema, 'vw_weather_city_daily_snapshot')} TO `{scientists}`",
            f"GRANT USE SCHEMA ON SCHEMA {fqn(catalog, gold_schema)} TO `{analysts}`",
            f"GRANT SELECT ON VIEW {fqn(catalog, gold_schema, 'vw_weather_city_daily_snapshot')} TO `{analysts}`",
        ]

        for statement in grant_statements:
            safe_execute(statement, errors=warnings)

    output_payload = {
        "status": "ok",
        "stage": "uc_governance_bootstrap",
        "catalog": catalog,
        "schemas": [ops_schema, bronze_schema, silver_schema, gold_schema, monitoring_schema],
        "silver_table": silver_table_fqn,
        "gold_table": gold_table_fqn,
        "checkpoint_table": checkpoint_table_fqn,
        "warnings": warnings,
        "errors": errors,
    }
    stage_success(output_payload)
except Exception as exc:
    if is_uc_unavailable_error(exc):
        stage_success(
            {
                "status": "ok",
                "stage": "uc_governance_bootstrap",
                "result": "skipped",
                "reason": "Unity Catalog not available in this workspace plan or permissions.",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "uc_settings": UC_SETTINGS,
            }
        )
    else:
        stage_error(
            "uc_governance_bootstrap",
            exc,
            context={
                "uc_settings": UC_SETTINGS,
                "errors": errors,
                "warnings": warnings,
            },
        )
