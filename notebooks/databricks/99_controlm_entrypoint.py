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

stage_timeout_seconds = int(dbutils.widgets.get("p_stage_timeout_seconds").strip() or "0")


def current_notebook_dir() -> str:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    notebook_path = context.notebookPath().get()
    return notebook_path.rsplit("/", 1)[0]


def build_base_args() -> dict[str, str]:
    return {widget_name: dbutils.widgets.get(widget_name) for widget_name in DEFAULT_WIDGETS}


base_dir = current_notebook_dir()
base_args = build_base_args()

def run_stage(stage_name: str, notebook_name: str, args: dict[str, str]) -> dict:
    notebook_path = f"{base_dir}/{notebook_name}"
    try:
        result_text = dbutils.notebook.run(notebook_path, stage_timeout_seconds, args)
        return json.loads(result_text)
    except Exception as exc:
        root_details = str(exc)
        raise RuntimeError(
            f"Stage '{stage_name}' failed in notebook '{notebook_path}'. "
            f"Root details: {root_details}"
        ) from exc


raw_result = run_stage("raw_bronze", "01_raw_bronze_ingestion", base_args)

silver_args = {
    **base_args,
    "p_run_id": raw_result["run_id"],
    "p_ingestion_date": raw_result["ingestion_date"],
    "p_ingestion_hour": raw_result["ingestion_hour"],
    "p_endpoint_for_silver": "weather",
}
silver_result = run_stage("silver", "02_silver_transform", silver_args)

gold_args = {
    **base_args,
    "p_run_id": raw_result["run_id"],
    "p_ingestion_date": raw_result["ingestion_date"],
    "p_ingestion_hour": raw_result["ingestion_hour"],
}
gold_result = run_stage("gold", "03_gold_transform", gold_args)

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
