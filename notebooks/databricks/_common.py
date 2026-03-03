# Databricks notebook source
# COMMAND ----------
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from pyspark.sql import functions as F


# COMMAND ----------
DEFAULT_WIDGETS = {
    "p_storage_protocol": "abfss",
    "p_storage_account": "tropowxdlprod",
    "p_container": "openweather-data",
    "p_openweather_base_url": "https://api.openweathermap.org/data/2.5",
    "p_openweather_endpoints": "weather",
    "p_openweather_units": "metric",
    "p_openweather_lang": "pt_br",
    "p_openweather_timeout_seconds": "30",
    "p_cities": "Sao Paulo,BR;Rio de Janeiro,BR;Curitiba,BR",
    "p_allow_plaintext_credentials": "false",
    "p_api_key": "",
    "p_openweather_secret_scope": "",
    "p_openweather_secret_key": "",
    "p_storage_account_key": "",
    "p_storage_secret_scope": "",
    "p_storage_secret_key": "",
}


# COMMAND ----------
def ensure_widget(name: str, default_value: str) -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default_value)


def ensure_base_widgets() -> None:
    for widget_name, widget_default in DEFAULT_WIDGETS.items():
        ensure_widget(widget_name, widget_default)


def parse_list(raw_value: str, separator: str) -> list[str]:
    if not raw_value:
        return []
    return [part.strip() for part in raw_value.split(separator) if part.strip()]


def resolve_secret_or_plain(
    *,
    plain_value: str,
    secret_scope: str,
    secret_key: str,
    required: bool,
    value_name: str,
) -> str:
    if plain_value:
        return plain_value
    if secret_scope and secret_key:
        return dbutils.secrets.get(secret_scope, secret_key)
    if required:
        raise ValueError(
            f"Missing {value_name}. Provide plain value or secret scope/key widgets."
        )
    return ""


def str_to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def get_spark_conf_or_empty(key: str) -> str:
    try:
        value = spark.conf.get(key)
        return value.strip() if value else ""
    except Exception:
        return ""


def get_hadoop_conf_or_empty(key: str) -> str:
    try:
        value = spark.sparkContext._jsc.hadoopConfiguration().get(key)
        if value is None:
            return ""
        return value.strip()
    except Exception:
        return ""


def resolve_openweather_api_key(*, require_api_key: bool) -> tuple[str, str]:
    allow_plain = str_to_bool(dbutils.widgets.get("p_allow_plaintext_credentials"))
    api_key_from_spark = get_spark_conf_or_empty("pipeline.openweather.api_key")
    if api_key_from_spark:
        return api_key_from_spark, "spark_conf:pipeline.openweather.api_key"

    secret_scope = dbutils.widgets.get("p_openweather_secret_scope").strip()
    secret_key = dbutils.widgets.get("p_openweather_secret_key").strip()
    if secret_scope and secret_key:
        return (
            dbutils.secrets.get(secret_scope, secret_key),
            f"secret:{secret_scope}/{secret_key}",
        )

    plain_api_key = dbutils.widgets.get("p_api_key").strip()
    if plain_api_key:
        if not allow_plain:
            raise ValueError(
                "p_api_key was provided but p_allow_plaintext_credentials=false. "
                "Use Job spark conf (pipeline.openweather.api_key) or secret scope/key widgets."
            )
        return plain_api_key, "widget:p_api_key"

    if require_api_key:
        raise ValueError(
            "Missing OpenWeather API key. Use one of: "
            "1) Job spark conf 'pipeline.openweather.api_key', "
            "2) p_openweather_secret_scope + p_openweather_secret_key, "
            "3) (manual only) p_api_key with p_allow_plaintext_credentials=true."
        )
    return "", "not_required"


def resolve_storage_account_key(storage_account: str) -> tuple[str, str]:
    allow_plain = str_to_bool(dbutils.widgets.get("p_allow_plaintext_credentials"))
    storage_key_from_spark = get_spark_conf_or_empty("pipeline.storage.account_key")
    if storage_key_from_spark:
        return storage_key_from_spark, "spark_conf:pipeline.storage.account_key"

    storage_key_from_dfs_spark_hadoop_conf = get_spark_conf_or_empty(
        f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net"
    )
    if storage_key_from_dfs_spark_hadoop_conf:
        return (
            storage_key_from_dfs_spark_hadoop_conf,
            f"spark_conf:spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        )

    storage_key_from_blob_spark_hadoop_conf = get_spark_conf_or_empty(
        f"spark.hadoop.fs.azure.account.key.{storage_account}.blob.core.windows.net"
    )
    if storage_key_from_blob_spark_hadoop_conf:
        return (
            storage_key_from_blob_spark_hadoop_conf,
            f"spark_conf:spark.hadoop.fs.azure.account.key.{storage_account}.blob.core.windows.net",
        )

    storage_key_from_dfs_conf = get_spark_conf_or_empty(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net"
    )
    if storage_key_from_dfs_conf:
        return (
            storage_key_from_dfs_conf,
            f"spark_conf:fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        )

    storage_key_from_blob_conf = get_spark_conf_or_empty(
        f"fs.azure.account.key.{storage_account}.blob.core.windows.net"
    )
    if storage_key_from_blob_conf:
        return (
            storage_key_from_blob_conf,
            f"spark_conf:fs.azure.account.key.{storage_account}.blob.core.windows.net",
        )

    storage_key_from_dfs_hadoop_conf = get_hadoop_conf_or_empty(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net"
    )
    if storage_key_from_dfs_hadoop_conf:
        return (
            storage_key_from_dfs_hadoop_conf,
            f"hadoop_conf:fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        )

    storage_key_from_blob_hadoop_conf = get_hadoop_conf_or_empty(
        f"fs.azure.account.key.{storage_account}.blob.core.windows.net"
    )
    if storage_key_from_blob_hadoop_conf:
        return (
            storage_key_from_blob_hadoop_conf,
            f"hadoop_conf:fs.azure.account.key.{storage_account}.blob.core.windows.net",
        )

    secret_scope = dbutils.widgets.get("p_storage_secret_scope").strip()
    secret_key = dbutils.widgets.get("p_storage_secret_key").strip()
    if secret_scope and secret_key:
        return (
            dbutils.secrets.get(secret_scope, secret_key),
            f"secret:{secret_scope}/{secret_key}",
        )

    plain_storage_key = dbutils.widgets.get("p_storage_account_key").strip()
    if plain_storage_key:
        if not allow_plain:
            raise ValueError(
                "p_storage_account_key was provided but p_allow_plaintext_credentials=false. "
                "Use Job spark conf (pipeline.storage.account_key) or secret scope/key widgets."
            )
        return plain_storage_key, "widget:p_storage_account_key"

    return "", "not_provided"


def get_runtime_config(*, require_api_key: bool = True) -> dict[str, Any]:
    ensure_base_widgets()

    storage_protocol = dbutils.widgets.get("p_storage_protocol").strip().lower() or "abfss"
    storage_account = dbutils.widgets.get("p_storage_account").strip()
    container = dbutils.widgets.get("p_container").strip()
    base_url = dbutils.widgets.get("p_openweather_base_url").strip().rstrip("/")
    endpoints = parse_list(dbutils.widgets.get("p_openweather_endpoints").strip(), ",")
    units = dbutils.widgets.get("p_openweather_units").strip() or "metric"
    lang = dbutils.widgets.get("p_openweather_lang").strip() or "pt_br"
    timeout_seconds = int(dbutils.widgets.get("p_openweather_timeout_seconds").strip())
    cities = parse_list(dbutils.widgets.get("p_cities").strip(), ";")

    if not storage_account:
        raise ValueError("Widget p_storage_account is required.")
    if not container:
        raise ValueError("Widget p_container is required.")
    if storage_protocol not in {"abfss", "wasbs"}:
        raise ValueError("Widget p_storage_protocol must be either 'abfss' or 'wasbs'.")
    if not endpoints:
        raise ValueError("Widget p_openweather_endpoints must contain at least one endpoint.")
    if not cities:
        raise ValueError("Widget p_cities must contain at least one city.")

    storage_endpoint = (
        "dfs.core.windows.net" if storage_protocol == "abfss" else "blob.core.windows.net"
    )
    api_key, api_key_source = resolve_openweather_api_key(require_api_key=require_api_key)
    storage_account_key, storage_key_source = resolve_storage_account_key(storage_account)
    base_uri = f"{storage_protocol}://{container}@{storage_account}.{storage_endpoint}"
    return {
        "storage_protocol": storage_protocol,
        "storage_endpoint": storage_endpoint,
        "storage_account": storage_account,
        "storage_account_key": storage_account_key,
        "storage_key_source": storage_key_source,
        "container": container,
        "base_uri": base_uri,
        "openweather_base_url": base_url,
        "openweather_endpoints": endpoints,
        "openweather_units": units,
        "openweather_lang": lang,
        "openweather_timeout_seconds": timeout_seconds,
        "cities": cities,
        "openweather_api_key": api_key,
        "openweather_api_key_source": api_key_source,
    }


def configure_storage_access(storage_account: str, storage_account_key: str) -> None:
    if storage_account_key:
        target_keys = [
            f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net",
            f"spark.hadoop.fs.azure.account.key.{storage_account}.blob.core.windows.net",
            f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
            f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
        ]
        for config_key in target_keys:
            try:
                spark.conf.set(config_key, storage_account_key)
            except Exception:
                pass

        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set(
            f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
            storage_account_key,
        )
        hadoop_conf.set(
            f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
            storage_account_key,
        )


# COMMAND ----------
def build_watermark() -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    return {
        "run_id": uuid.uuid4().hex,
        "ingestion_ts_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "ingestion_epoch": int(now_utc.timestamp()),
        "ingestion_date": now_utc.strftime("%Y-%m-%d"),
        "ingestion_hour": now_utc.strftime("%H"),
    }


def sanitize_request_url(request_url: str) -> str:
    split_url = urlsplit(request_url)
    query_pairs = parse_qsl(split_url.query, keep_blank_values=True)
    sanitized_pairs = [
        (key, "***") if key.lower() == "appid" else (key, value)
        for key, value in query_pairs
    ]
    return urlunsplit(
        (
            split_url.scheme,
            split_url.netloc,
            split_url.path,
            urlencode(sanitized_pairs),
            split_url.fragment,
        )
    )


def slugify_city(city: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", city).strip("_").lower() or "unknown"


def epoch_to_iso_utc(epoch_value: Any) -> str | None:
    if epoch_value is None:
        return None
    return datetime.fromtimestamp(int(epoch_value), timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def fetch_openweather_payload(config: dict[str, Any], endpoint: str, city: str) -> dict[str, Any]:
    url = f"{config['openweather_base_url']}/{endpoint}"
    params = {
        "q": city,
        "appid": config["openweather_api_key"],
        "units": config["openweather_units"],
        "lang": config["openweather_lang"],
    }
    response = requests.get(url, params=params, timeout=config["openweather_timeout_seconds"])
    payload = response.json()
    if response.status_code >= 400:
        error_message = payload.get("message", "unknown error")
        raise RuntimeError(
            f"OpenWeather HTTP {response.status_code} for endpoint={endpoint}, city={city}: {error_message}"
        )
    return {
        "request_url": response.url,
        "status_code": response.status_code,
        "payload": payload,
    }


def build_raw_record(
    *,
    endpoint: str,
    city_query: str,
    status_code: int,
    request_url: str,
    payload: dict[str, Any],
    watermark: dict[str, Any],
) -> dict[str, Any]:
    source_epoch = int(payload.get("dt") or watermark["ingestion_epoch"])
    return {
        "metadata": {
            "watermark_run_id": watermark["run_id"],
            "watermark_ingestion_ts_utc": watermark["ingestion_ts_utc"],
            "watermark_ingestion_epoch": watermark["ingestion_epoch"],
            "watermark_source_epoch": source_epoch,
            "watermark_source_ts_utc": epoch_to_iso_utc(source_epoch),
            "endpoint": endpoint,
            "city_query": city_query,
            "request_url": sanitize_request_url(request_url),
            "status_code": status_code,
        },
        "payload": payload,
    }


def build_bronze_record(
    *,
    endpoint: str,
    city_query: str,
    payload: dict[str, Any],
    watermark: dict[str, Any],
) -> dict[str, Any]:
    source_epoch = int(payload.get("dt") or watermark["ingestion_epoch"])
    weather = (payload.get("weather") or [{}])[0]
    coord = payload.get("coord", {})
    main = payload.get("main", {})
    wind = payload.get("wind", {})
    clouds = payload.get("clouds", {})
    system = payload.get("sys", {})
    return {
        "watermark_run_id": watermark["run_id"],
        "watermark_ingestion_ts_utc": watermark["ingestion_ts_utc"],
        "watermark_ingestion_epoch": watermark["ingestion_epoch"],
        "watermark_source_epoch": source_epoch,
        "watermark_source_ts_utc": epoch_to_iso_utc(source_epoch),
        "endpoint": endpoint,
        "city_query": city_query,
        "city_id": payload.get("id"),
        "city_name": payload.get("name"),
        "country": system.get("country"),
        "latitude": coord.get("lat"),
        "longitude": coord.get("lon"),
        "observation_epoch": payload.get("dt"),
        "observation_ts_utc": epoch_to_iso_utc(payload.get("dt")),
        "timezone_offset_seconds": payload.get("timezone"),
        "weather_main": weather.get("main"),
        "weather_description": weather.get("description"),
        "temperature_celsius": main.get("temp"),
        "feels_like_celsius": main.get("feels_like"),
        "temp_min_celsius": main.get("temp_min"),
        "temp_max_celsius": main.get("temp_max"),
        "pressure_hpa": main.get("pressure"),
        "humidity_pct": main.get("humidity"),
        "wind_speed_ms": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "cloudiness_pct": clouds.get("all"),
        "sunrise_ts_utc": epoch_to_iso_utc(system.get("sunrise")),
        "sunset_ts_utc": epoch_to_iso_utc(system.get("sunset")),
        "source_payload": payload,
    }


def build_record_path(
    *,
    base_uri: str,
    layer: str,
    endpoint: str,
    city_slug: str,
    watermark: dict[str, Any],
    extension: str,
) -> str:
    return (
        f"{base_uri}/{layer}/openweather/{endpoint}/"
        f"ingestion_date={watermark['ingestion_date']}/"
        f"ingestion_hour={watermark['ingestion_hour']}/"
        f"city={city_slug}/"
        f"run_id={watermark['run_id']}.{extension}"
    )


def write_json_document(path: str, payload: dict[str, Any], compact: bool = False) -> None:
    content = (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        if compact
        else json.dumps(payload, ensure_ascii=False, indent=2)
    )
    try:
        dbutils.fs.put(path, content, True)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to write JSON to path '{path}'. "
            "Check storage protocol, container name, and storage credentials."
        ) from exc


def storage_preflight_check(config: dict[str, Any], watermark: dict[str, Any]) -> str:
    check_path = (
        f"{config['base_uri']}/raw/openweather/_control/healthcheck/"
        f"run_id={watermark['run_id']}.json"
    )
    check_payload = {
        "status": "ok",
        "check": "storage_write",
        "storage_protocol": config["storage_protocol"],
        "storage_account": config["storage_account"],
        "container": config["container"],
        "run_id": watermark["run_id"],
        "ingestion_ts_utc": watermark["ingestion_ts_utc"],
    }
    write_json_document(check_path, check_payload, compact=False)
    return check_path


def write_run_manifest(base_uri: str, manifest: dict[str, Any]) -> str:
    manifest_path = (
        f"{base_uri}/raw/openweather/_control/runs/"
        f"{manifest['ingestion_epoch']}_{manifest['run_id']}.json"
    )
    write_json_document(manifest_path, manifest, compact=False)
    return manifest_path


def load_latest_run_manifest(base_uri: str) -> tuple[dict[str, Any], str]:
    control_path = f"{base_uri}/raw/openweather/_control/runs"
    entries = [entry for entry in dbutils.fs.ls(control_path) if entry.path.endswith(".json")]
    if not entries:
        raise ValueError(f"No run manifest found under {control_path}.")
    latest_entry = max(entries, key=lambda entry: entry.modificationTime)
    content = dbutils.fs.head(latest_entry.path, 2_000_000)
    return json.loads(content), latest_entry.path


def resolve_run_context(
    *,
    base_uri: str,
    run_id: str,
    ingestion_date: str,
    ingestion_hour: str,
) -> dict[str, Any]:
    if run_id and ingestion_date and ingestion_hour:
        return {
            "run_id": run_id,
            "ingestion_date": ingestion_date,
            "ingestion_hour": ingestion_hour,
            "source": "widgets",
        }

    manifest, manifest_path = load_latest_run_manifest(base_uri)
    return {
        "run_id": manifest["run_id"],
        "ingestion_date": manifest["ingestion_date"],
        "ingestion_hour": manifest["ingestion_hour"],
        "source": "latest_manifest",
        "manifest_path": manifest_path,
    }
