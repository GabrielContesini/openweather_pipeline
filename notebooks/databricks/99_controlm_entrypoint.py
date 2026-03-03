# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Control-M Entrypoint
# MAGIC
# MAGIC Orchestrates the full medallion flow by calling:
# MAGIC
# MAGIC 1. `01_raw_bronze_ingestion`
# MAGIC 2. `02_silver_transform`
# MAGIC 3. `03_gold_transform`

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
ensure_widget("p_stage_timeout_seconds", "0")

for widget_name, widget_default in DEFAULT_WIDGETS.items():
    ensure_widget(widget_name, widget_default)

config = get_runtime_config()
configure_storage_access(config["storage_account"], config["storage_account_key"])
stage_timeout_seconds = int(dbutils.widgets.get("p_stage_timeout_seconds").strip() or "0")


def current_notebook_dir() -> str:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    notebook_path = context.notebookPath().get()
    return notebook_path.rsplit("/", 1)[0]


def build_base_args() -> dict[str, str]:
    return {widget_name: dbutils.widgets.get(widget_name) for widget_name in DEFAULT_WIDGETS}


base_dir = current_notebook_dir()
base_args = build_base_args()

raw_result = json.loads(
    dbutils.notebook.run(
        f"{base_dir}/01_raw_bronze_ingestion",
        stage_timeout_seconds,
        base_args,
    )
)

silver_args = {
    **base_args,
    "p_run_id": raw_result["run_id"],
    "p_ingestion_date": raw_result["ingestion_date"],
    "p_ingestion_hour": raw_result["ingestion_hour"],
    "p_endpoint_for_silver": "weather",
}
silver_result = json.loads(
    dbutils.notebook.run(
        f"{base_dir}/02_silver_transform",
        stage_timeout_seconds,
        silver_args,
    )
)

gold_args = {
    **base_args,
    "p_run_id": raw_result["run_id"],
    "p_ingestion_date": raw_result["ingestion_date"],
    "p_ingestion_hour": raw_result["ingestion_hour"],
}
gold_result = json.loads(
    dbutils.notebook.run(
        f"{base_dir}/03_gold_transform",
        stage_timeout_seconds,
        gold_args,
    )
)

pipeline_result = {
    "status": "ok",
    "run_id": raw_result["run_id"],
    "ingestion_date": raw_result["ingestion_date"],
    "ingestion_hour": raw_result["ingestion_hour"],
    "raw_bronze": raw_result,
    "silver": silver_result,
    "gold": gold_result,
}

print(json.dumps(pipeline_result, indent=2))
dbutils.notebook.exit(json.dumps(pipeline_result))
