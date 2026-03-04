# Databricks notebook source
# COMMAND ----------
import json
import re
import subprocess
import sys
import traceback
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
import requests

try:
    from azure.core.exceptions import ResourceExistsError
    from azure.storage.blob import BlobServiceClient, ContentSettings
except ImportError as exc:
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "azure-storage-blob", "pyarrow", "-q"]
        )
        from azure.core.exceptions import ResourceExistsError
        from azure.storage.blob import BlobServiceClient, ContentSettings
    except Exception as install_exc:
        raise ImportError(
            "Missing dependency: azure-storage-blob/pyarrow and auto-install failed. "
            "Install these packages in the Databricks environment."
        ) from install_exc


# COMMAND ----------
DEFAULT_WIDGETS = {
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
    "p_storage_connection_string": "",
    "p_storage_connection_string_secret_scope": "",
    "p_storage_connection_string_secret_key": "",
    "p_storage_account_key": "",
    "p_storage_secret_scope": "",
    "p_storage_secret_key": "",
    "p_storage_sas_token": "",
    "p_storage_sas_secret_scope": "",
    "p_storage_sas_secret_key": "",
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


def str_to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def get_spark_conf_or_empty(key: str) -> str:
    try:
        value = spark.conf.get(key)
        return value.strip() if value else ""
    except Exception:
        return ""


def get_secret_or_empty(secret_scope: str, secret_key: str) -> str:
    if not secret_scope or not secret_key:
        return ""
    return dbutils.secrets.get(secret_scope, secret_key)


def require_plaintext_opt_in(widget_name: str) -> None:
    if not str_to_bool(dbutils.widgets.get("p_allow_plaintext_credentials")):
        raise ValueError(
            f"{widget_name} informed but p_allow_plaintext_credentials=false. "
            "Use spark conf or secret scope/key."
        )


def resolve_openweather_api_key(*, require_api_key: bool) -> tuple[str, str]:
    api_key_from_spark = get_spark_conf_or_empty("pipeline.openweather.api_key")
    if api_key_from_spark:
        return api_key_from_spark, "spark_conf:pipeline.openweather.api_key"

    scope = dbutils.widgets.get("p_openweather_secret_scope").strip()
    key = dbutils.widgets.get("p_openweather_secret_key").strip()
    api_key_from_secret = get_secret_or_empty(scope, key)
    if api_key_from_secret:
        return api_key_from_secret, f"secret:{scope}/{key}"

    api_key_plain = dbutils.widgets.get("p_api_key").strip()
    if api_key_plain:
        require_plaintext_opt_in("p_api_key")
        return api_key_plain, "widget:p_api_key"

    if require_api_key:
        raise ValueError(
            "Missing OpenWeather API key. Use: "
            "1) spark conf pipeline.openweather.api_key, "
            "2) p_openweather_secret_scope + p_openweather_secret_key, "
            "3) (manual) p_api_key with p_allow_plaintext_credentials=true."
        )
    return "", "not_required"


def resolve_storage_auth() -> dict[str, str]:
    conn_string_from_spark = get_spark_conf_or_empty("pipeline.storage.connection_string")
    if conn_string_from_spark:
        return {
            "auth_type": "connection_string",
            "credential": conn_string_from_spark,
            "source": "spark_conf:pipeline.storage.connection_string",
        }

    conn_scope = dbutils.widgets.get("p_storage_connection_string_secret_scope").strip()
    conn_key = dbutils.widgets.get("p_storage_connection_string_secret_key").strip()
    conn_string_from_secret = get_secret_or_empty(conn_scope, conn_key)
    if conn_string_from_secret:
        return {
            "auth_type": "connection_string",
            "credential": conn_string_from_secret,
            "source": f"secret:{conn_scope}/{conn_key}",
        }

    conn_string_plain = dbutils.widgets.get("p_storage_connection_string").strip()
    if conn_string_plain:
        require_plaintext_opt_in("p_storage_connection_string")
        return {
            "auth_type": "connection_string",
            "credential": conn_string_plain,
            "source": "widget:p_storage_connection_string",
        }

    account_key_from_spark = get_spark_conf_or_empty("pipeline.storage.account_key")
    if account_key_from_spark:
        return {
            "auth_type": "credential",
            "credential": account_key_from_spark,
            "credential_kind": "account_key",
            "source": "spark_conf:pipeline.storage.account_key",
        }

    key_scope = dbutils.widgets.get("p_storage_secret_scope").strip()
    key_name = dbutils.widgets.get("p_storage_secret_key").strip()
    account_key_from_secret = get_secret_or_empty(key_scope, key_name)
    if account_key_from_secret:
        return {
            "auth_type": "credential",
            "credential": account_key_from_secret,
            "credential_kind": "account_key",
            "source": f"secret:{key_scope}/{key_name}",
        }

    account_key_plain = dbutils.widgets.get("p_storage_account_key").strip()
    if account_key_plain:
        require_plaintext_opt_in("p_storage_account_key")
        return {
            "auth_type": "credential",
            "credential": account_key_plain,
            "credential_kind": "account_key",
            "source": "widget:p_storage_account_key",
        }

    sas_token_from_spark = get_spark_conf_or_empty("pipeline.storage.sas_token")
    if sas_token_from_spark:
        return {
            "auth_type": "credential",
            "credential": sas_token_from_spark,
            "credential_kind": "sas_token",
            "source": "spark_conf:pipeline.storage.sas_token",
        }

    sas_scope = dbutils.widgets.get("p_storage_sas_secret_scope").strip()
    sas_key = dbutils.widgets.get("p_storage_sas_secret_key").strip()
    sas_token_from_secret = get_secret_or_empty(sas_scope, sas_key)
    if sas_token_from_secret:
        return {
            "auth_type": "credential",
            "credential": sas_token_from_secret,
            "credential_kind": "sas_token",
            "source": f"secret:{sas_scope}/{sas_key}",
        }

    sas_token_plain = dbutils.widgets.get("p_storage_sas_token").strip()
    if sas_token_plain:
        require_plaintext_opt_in("p_storage_sas_token")
        return {
            "auth_type": "credential",
            "credential": sas_token_plain,
            "credential_kind": "sas_token",
            "source": "widget:p_storage_sas_token",
        }

    raise ValueError(
        "Missing storage auth. Use one of: "
        "1) connection string via spark conf/secret, "
        "2) account key via spark conf/secret, "
        "3) SAS token via spark conf/secret."
    )


def get_runtime_config(*, require_api_key: bool = True) -> dict[str, Any]:
    ensure_base_widgets()

    storage_account = dbutils.widgets.get("p_storage_account").strip()
    container = dbutils.widgets.get("p_container").strip()
    if not storage_account:
        raise ValueError("Widget p_storage_account is required.")
    if not container:
        raise ValueError("Widget p_container is required.")

    api_key, api_key_source = resolve_openweather_api_key(require_api_key=require_api_key)
    storage_auth = resolve_storage_auth()
    return {
        "storage_account": storage_account,
        "container": container,
        "storage_account_url": f"https://{storage_account}.blob.core.windows.net",
        "storage_auth": storage_auth,
        "openweather_base_url": dbutils.widgets.get("p_openweather_base_url").strip().rstrip("/"),
        "openweather_endpoints": parse_list(
            dbutils.widgets.get("p_openweather_endpoints").strip(), ","
        ),
        "openweather_units": dbutils.widgets.get("p_openweather_units").strip() or "metric",
        "openweather_lang": dbutils.widgets.get("p_openweather_lang").strip() or "pt_br",
        "openweather_timeout_seconds": int(
            dbutils.widgets.get("p_openweather_timeout_seconds").strip()
        ),
        "cities": parse_list(dbutils.widgets.get("p_cities").strip(), ";"),
        "openweather_api_key": api_key,
        "openweather_api_key_source": api_key_source,
    }


def build_blob_service_client(config: dict[str, Any]) -> BlobServiceClient:
    storage_auth = config["storage_auth"]
    if storage_auth["auth_type"] == "connection_string":
        return BlobServiceClient.from_connection_string(storage_auth["credential"])
    return BlobServiceClient(
        account_url=config["storage_account_url"],
        credential=storage_auth["credential"],
    )


def get_container_client(config: dict[str, Any], *, create_if_missing: bool) -> Any:
    service_client = build_blob_service_client(config)
    container_client = service_client.get_container_client(config["container"])
    if create_if_missing:
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass
    return container_client


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


def bronze_to_silver_row(bronze_record: dict[str, Any]) -> dict[str, Any]:
    observation_ts_utc = bronze_record.get("observation_ts_utc")
    event_date = observation_ts_utc[:10] if observation_ts_utc else None
    return {
        "watermark_run_id": bronze_record.get("watermark_run_id"),
        "watermark_ingestion_ts_utc": bronze_record.get("watermark_ingestion_ts_utc"),
        "watermark_ingestion_epoch": bronze_record.get("watermark_ingestion_epoch"),
        "watermark_source_epoch": bronze_record.get("watermark_source_epoch"),
        "watermark_source_ts_utc": bronze_record.get("watermark_source_ts_utc"),
        "event_date": event_date,
        "city_id": bronze_record.get("city_id"),
        "city_name": bronze_record.get("city_name"),
        "country": bronze_record.get("country"),
        "latitude": bronze_record.get("latitude"),
        "longitude": bronze_record.get("longitude"),
        "observation_epoch": bronze_record.get("observation_epoch"),
        "observation_ts_utc": bronze_record.get("observation_ts_utc"),
        "timezone_offset_seconds": bronze_record.get("timezone_offset_seconds"),
        "weather_main": bronze_record.get("weather_main"),
        "weather_description": bronze_record.get("weather_description"),
        "temperature_celsius": bronze_record.get("temperature_celsius"),
        "feels_like_celsius": bronze_record.get("feels_like_celsius"),
        "temp_min_celsius": bronze_record.get("temp_min_celsius"),
        "temp_max_celsius": bronze_record.get("temp_max_celsius"),
        "pressure_hpa": bronze_record.get("pressure_hpa"),
        "humidity_pct": bronze_record.get("humidity_pct"),
        "wind_speed_ms": bronze_record.get("wind_speed_ms"),
        "wind_deg": bronze_record.get("wind_deg"),
        "cloudiness_pct": bronze_record.get("cloudiness_pct"),
        "sunrise_ts_utc": bronze_record.get("sunrise_ts_utc"),
        "sunset_ts_utc": bronze_record.get("sunset_ts_utc"),
    }


def build_record_blob_path(
    *,
    layer: str,
    endpoint: str,
    city_query: str,
    watermark: dict[str, Any],
    extension: str,
) -> str:
    city_slug = slugify_city(city_query)
    return (
        f"{layer}/openweather/{endpoint}/"
        f"ingestion_date={watermark['ingestion_date']}/"
        f"ingestion_hour={watermark['ingestion_hour']}/"
        f"city={city_slug}/"
        f"run_id={watermark['run_id']}.{extension}"
    )


def build_dataset_blob_path(
    *,
    layer: str,
    dataset: str,
    run_id: str,
    ingestion_date: str,
    ingestion_hour: str,
    extension: str,
) -> str:
    return (
        f"{layer}/openweather/{dataset}/"
        f"ingestion_date={ingestion_date}/"
        f"ingestion_hour={ingestion_hour}/"
        f"run_id={run_id}/"
        f"part-00000.{extension}"
    )


def build_blob_metadata(layer: str, watermark: dict[str, Any]) -> dict[str, str]:
    return {
        "pipeline": "openweather_medallion_databricks",
        "layer": layer,
        "run_id": str(watermark.get("run_id", "")),
        "ingestion_ts": str(watermark.get("ingestion_ts_utc", "")),
        "ingestion_epoch": str(watermark.get("ingestion_epoch", "")),
    }


def upload_json_blob(
    container_client: Any,
    blob_path: str,
    payload: dict[str, Any],
    *,
    compact: bool,
    metadata: dict[str, str],
) -> None:
    content = (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        if compact
        else json.dumps(payload, ensure_ascii=False, indent=2)
    ).encode("utf-8")
    container_client.get_blob_client(blob_path).upload_blob(
        content,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
        metadata=metadata,
    )


def download_json_blob(container_client: Any, blob_path: str) -> dict[str, Any]:
    content = container_client.get_blob_client(blob_path).download_blob().readall()
    return json.loads(content.decode("utf-8"))


def upload_parquet_blob(
    container_client: Any,
    blob_path: str,
    dataframe: pd.DataFrame,
    *,
    metadata: dict[str, str],
) -> None:
    output = BytesIO()
    dataframe.to_parquet(output, index=False, engine="pyarrow")
    container_client.get_blob_client(blob_path).upload_blob(
        output.getvalue(),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/octet-stream"),
        metadata=metadata,
    )


def download_parquet_blob(container_client: Any, blob_path: str) -> pd.DataFrame:
    data = container_client.get_blob_client(blob_path).download_blob().readall()
    return pd.read_parquet(BytesIO(data))


def list_blob_names(container_client: Any, prefix: str) -> list[str]:
    return [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]


def storage_preflight_check(
    container_client: Any, config: dict[str, Any], watermark: dict[str, Any]
) -> str:
    check_path = (
        f"raw/openweather/_control/healthcheck/"
        f"run_id={watermark['run_id']}.json"
    )
    check_payload = {
        "status": "ok",
        "check": "storage_write",
        "storage_account": config["storage_account"],
        "container": config["container"],
        "run_id": watermark["run_id"],
        "ingestion_ts_utc": watermark["ingestion_ts_utc"],
    }
    upload_json_blob(
        container_client,
        check_path,
        check_payload,
        compact=False,
        metadata=build_blob_metadata("raw", watermark),
    )
    return check_path


def write_run_manifest(container_client: Any, manifest: dict[str, Any]) -> str:
    manifest_path = (
        "raw/openweather/_control/runs/"
        f"{manifest['ingestion_epoch']}_{manifest['run_id']}.json"
    )
    upload_json_blob(
        container_client,
        manifest_path,
        manifest,
        compact=False,
        metadata=build_blob_metadata(
            "raw",
            {
                "run_id": manifest["run_id"],
                "ingestion_ts_utc": manifest["ingestion_ts_utc"],
                "ingestion_epoch": manifest["ingestion_epoch"],
            },
        ),
    )
    return manifest_path


def load_latest_run_manifest(container_client: Any) -> tuple[dict[str, Any], str]:
    prefix = "raw/openweather/_control/runs/"
    blobs = list(container_client.list_blobs(name_starts_with=prefix))
    if not blobs:
        raise ValueError(f"No run manifest found under prefix '{prefix}'.")
    latest_blob = max(blobs, key=lambda blob: blob.last_modified)
    return download_json_blob(container_client, latest_blob.name), latest_blob.name


def resolve_run_context(
    container_client: Any,
    *,
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

    manifest, manifest_path = load_latest_run_manifest(container_client)
    return {
        "run_id": manifest["run_id"],
        "ingestion_date": manifest["ingestion_date"],
        "ingestion_hour": manifest["ingestion_hour"],
        "source": "latest_manifest",
        "manifest_path": manifest_path,
    }


def coerce_numeric_columns(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
    return dataframe


def stage_success(payload: dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    dbutils.notebook.exit(text)


def stage_error(stage: str, exc: Exception, context: dict[str, Any] | None = None) -> None:
    payload = {
        "status": "error",
        "stage": stage,
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "traceback": traceback.format_exc(),
    }
    if context:
        payload["context"] = context

    text = json.dumps(payload, ensure_ascii=False)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    dbutils.notebook.exit(text)
