from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings

from openweather_client import OpenWeatherClient, OpenWeatherResponse
from settings import PipelineSettings


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class Watermark:
    run_id: str
    ingestion_ts_utc: str
    ingestion_epoch: int
    ingestion_date: str
    ingestion_hour: str

    @classmethod
    def new(cls) -> "Watermark":
        now_utc = datetime.now(UTC)
        return cls(
            run_id=uuid4().hex,
            ingestion_ts_utc=now_utc.isoformat().replace("+00:00", "Z"),
            ingestion_epoch=int(now_utc.timestamp()),
            ingestion_date=now_utc.strftime("%Y-%m-%d"),
            ingestion_hour=now_utc.strftime("%H"),
        )


@dataclass
class PipelineResult:
    run_id: str
    raw_records: int = 0
    bronze_records: int = 0
    silver_rows: int = 0
    gold_rows: int = 0
    uploaded_blobs: list[str] = field(default_factory=list)
    local_files: list[Path] = field(default_factory=list)


def _slugify(value: str) -> str:
    safe_value = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip())
    return safe_value.strip("_").lower() or "unknown"


def _epoch_to_iso_utc(epoch_value: int | None) -> str | None:
    if epoch_value is None:
        return None
    return datetime.fromtimestamp(epoch_value, UTC).isoformat().replace("+00:00", "Z")


def _to_parquet_bytes(dataframe: pd.DataFrame) -> bytes:
    output = BytesIO()
    dataframe.to_parquet(output, index=False, engine="pyarrow")
    return output.getvalue()


def _sanitize_request_url(request_url: str) -> str:
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


class LocalArtifactSink:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write(self, relative_blob_path: str, payload: bytes) -> Path:
        file_path = self.base_dir / relative_blob_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(payload)
        return file_path


class AzureStorageSink:
    def __init__(self, settings: PipelineSettings) -> None:
        self.settings = settings
        self.service_client = self._build_client(settings)
        self.container_client = self.service_client.get_container_client(
            settings.azure_storage_container
        )
        self._ensure_container()

    @staticmethod
    def _build_client(settings: PipelineSettings) -> BlobServiceClient:
        if settings.azure_storage_connection_string:
            return BlobServiceClient.from_connection_string(
                settings.azure_storage_connection_string
            )
        credential = settings.azure_storage_account_key or settings.azure_storage_sas_token
        return BlobServiceClient(
            account_url=settings.azure_storage_account_url,
            credential=credential,
        )

    def _ensure_container(self) -> None:
        try:
            self.container_client.create_container()
            LOGGER.info("Container criado: %s", self.settings.azure_storage_container)
        except ResourceExistsError:
            LOGGER.info("Container existente: %s", self.settings.azure_storage_container)

    def upload(
        self,
        *,
        blob_path: str,
        payload: bytes,
        content_type: str,
        metadata: dict[str, str],
    ) -> str:
        blob_client = self.container_client.get_blob_client(blob_path)
        blob_client.upload_blob(
            payload,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
            metadata=metadata,
        )
        return blob_client.url


class OpenWeatherMedallionPipeline:
    def __init__(
        self,
        *,
        settings: PipelineSettings,
        local_only: bool = False,
    ) -> None:
        self.settings = settings
        self.local_only = local_only
        self.client = OpenWeatherClient(
            base_url=settings.openweather_base_url,
            api_key=settings.openweather_api_key,
            timeout_seconds=settings.request_timeout_seconds,
        )
        self.local_sink = LocalArtifactSink(settings.local_output_dir)
        self.azure_sink = None if local_only else AzureStorageSink(settings)

    def run(self) -> PipelineResult:
        watermark = Watermark.new()
        result = PipelineResult(run_id=watermark.run_id)
        silver_rows: list[dict] = []

        for endpoint in self.settings.openweather_endpoints:
            for city in self.settings.cities:
                response = self.client.get_weather(
                    endpoint=endpoint,
                    city=city,
                    units=self.settings.openweather_units,
                    lang=self.settings.openweather_lang,
                )
                source_epoch = int(
                    response.payload.get("dt") or watermark.ingestion_epoch
                )

                raw_record = self._build_raw_record(
                    response=response,
                    watermark=watermark,
                    source_epoch=source_epoch,
                )
                raw_blob_path = self._record_blob_path(
                    layer="raw",
                    endpoint=endpoint,
                    city=city,
                    watermark=watermark,
                    extension="json",
                )
                self._persist_artifact(
                    result=result,
                    layer="raw",
                    blob_path=raw_blob_path,
                    payload=json.dumps(raw_record, ensure_ascii=False, indent=2).encode(
                        "utf-8"
                    ),
                    content_type="application/json",
                    watermark=watermark,
                )
                result.raw_records += 1

                bronze_record = self._build_bronze_record(
                    response=response,
                    watermark=watermark,
                    source_epoch=source_epoch,
                )
                bronze_blob_path = self._record_blob_path(
                    layer="bronze",
                    endpoint=endpoint,
                    city=city,
                    watermark=watermark,
                    extension="json",
                )
                self._persist_artifact(
                    result=result,
                    layer="bronze",
                    blob_path=bronze_blob_path,
                    payload=json.dumps(
                        bronze_record, ensure_ascii=False, separators=(",", ":")
                    ).encode("utf-8"),
                    content_type="application/json",
                    watermark=watermark,
                )
                result.bronze_records += 1

                if endpoint == "weather":
                    silver_rows.append(self._build_silver_row(bronze_record))
                else:
                    LOGGER.warning(
                        "Endpoint %s ainda sem mapeamento Silver/Gold; processado apenas raw/bronze.",
                        endpoint,
                    )

        if not silver_rows:
            return result

        silver_df = pd.DataFrame(silver_rows)
        result.silver_rows = len(silver_df)
        silver_blob_path = self._dataset_blob_path(
            layer="silver",
            dataset="openweather_current_weather",
            watermark=watermark,
            extension="parquet",
        )
        self._persist_artifact(
            result=result,
            layer="silver",
            blob_path=silver_blob_path,
            payload=_to_parquet_bytes(silver_df),
            content_type="application/octet-stream",
            watermark=watermark,
        )

        gold_df = self._build_gold_df(silver_df, watermark)
        result.gold_rows = len(gold_df)
        gold_blob_path = self._dataset_blob_path(
            layer="gold",
            dataset="weather_city_daily_snapshot",
            watermark=watermark,
            extension="parquet",
        )
        self._persist_artifact(
            result=result,
            layer="gold",
            blob_path=gold_blob_path,
            payload=_to_parquet_bytes(gold_df),
            content_type="application/octet-stream",
            watermark=watermark,
        )

        return result

    @staticmethod
    def _build_raw_record(
        *,
        response: OpenWeatherResponse,
        watermark: Watermark,
        source_epoch: int,
    ) -> dict:
        return {
            "metadata": {
                "watermark_run_id": watermark.run_id,
                "watermark_ingestion_ts_utc": watermark.ingestion_ts_utc,
                "watermark_ingestion_epoch": watermark.ingestion_epoch,
                "watermark_source_epoch": source_epoch,
                "watermark_source_ts_utc": _epoch_to_iso_utc(source_epoch),
                "endpoint": response.endpoint,
                "city_query": response.city_query,
                "request_url": _sanitize_request_url(response.request_url),
                "status_code": response.status_code,
            },
            "payload": response.payload,
        }

    @staticmethod
    def _build_bronze_record(
        *,
        response: OpenWeatherResponse,
        watermark: Watermark,
        source_epoch: int,
    ) -> dict:
        payload = response.payload
        weather = (payload.get("weather") or [{}])[0]
        coord = payload.get("coord", {})
        main = payload.get("main", {})
        wind = payload.get("wind", {})
        clouds = payload.get("clouds", {})
        system = payload.get("sys", {})

        return {
            "watermark_run_id": watermark.run_id,
            "watermark_ingestion_ts_utc": watermark.ingestion_ts_utc,
            "watermark_ingestion_epoch": watermark.ingestion_epoch,
            "watermark_source_epoch": source_epoch,
            "watermark_source_ts_utc": _epoch_to_iso_utc(source_epoch),
            "endpoint": response.endpoint,
            "city_query": response.city_query,
            "city_id": payload.get("id"),
            "city_name": payload.get("name"),
            "country": system.get("country"),
            "latitude": coord.get("lat"),
            "longitude": coord.get("lon"),
            "observation_epoch": payload.get("dt"),
            "observation_ts_utc": _epoch_to_iso_utc(payload.get("dt")),
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
            "sunrise_ts_utc": _epoch_to_iso_utc(system.get("sunrise")),
            "sunset_ts_utc": _epoch_to_iso_utc(system.get("sunset")),
            "source_payload": payload,
        }

    @staticmethod
    def _build_silver_row(bronze_record: dict) -> dict:
        observation_ts_utc = bronze_record.get("observation_ts_utc")
        event_date = observation_ts_utc[:10] if observation_ts_utc else None

        return {
            "watermark_run_id": bronze_record["watermark_run_id"],
            "watermark_ingestion_ts_utc": bronze_record["watermark_ingestion_ts_utc"],
            "watermark_ingestion_epoch": bronze_record["watermark_ingestion_epoch"],
            "watermark_source_epoch": bronze_record["watermark_source_epoch"],
            "event_date": event_date,
            "city_id": bronze_record["city_id"],
            "city_name": bronze_record["city_name"],
            "country": bronze_record["country"],
            "latitude": bronze_record["latitude"],
            "longitude": bronze_record["longitude"],
            "observation_epoch": bronze_record["observation_epoch"],
            "observation_ts_utc": bronze_record["observation_ts_utc"],
            "timezone_offset_seconds": bronze_record["timezone_offset_seconds"],
            "weather_main": bronze_record["weather_main"],
            "weather_description": bronze_record["weather_description"],
            "temperature_celsius": bronze_record["temperature_celsius"],
            "feels_like_celsius": bronze_record["feels_like_celsius"],
            "temp_min_celsius": bronze_record["temp_min_celsius"],
            "temp_max_celsius": bronze_record["temp_max_celsius"],
            "pressure_hpa": bronze_record["pressure_hpa"],
            "humidity_pct": bronze_record["humidity_pct"],
            "wind_speed_ms": bronze_record["wind_speed_ms"],
            "wind_deg": bronze_record["wind_deg"],
            "cloudiness_pct": bronze_record["cloudiness_pct"],
            "sunrise_ts_utc": bronze_record["sunrise_ts_utc"],
            "sunset_ts_utc": bronze_record["sunset_ts_utc"],
        }

    @staticmethod
    def _build_gold_df(silver_df: pd.DataFrame, watermark: Watermark) -> pd.DataFrame:
        grouped = (
            silver_df.groupby(["event_date", "city_name", "country"], dropna=False)
            .agg(
                records_count=("city_id", "count"),
                temperature_avg_celsius=("temperature_celsius", "mean"),
                temperature_min_celsius=("temp_min_celsius", "min"),
                temperature_max_celsius=("temp_max_celsius", "max"),
                humidity_avg_pct=("humidity_pct", "mean"),
                wind_speed_avg_ms=("wind_speed_ms", "mean"),
                last_observation_ts_utc=("observation_ts_utc", "max"),
            )
            .reset_index()
        )

        grouped["watermark_run_id"] = watermark.run_id
        grouped["watermark_ingestion_ts_utc"] = watermark.ingestion_ts_utc
        grouped["watermark_ingestion_epoch"] = watermark.ingestion_epoch
        grouped["watermark_source_max_epoch"] = silver_df["observation_epoch"].max()
        grouped["watermark_source_max_ts_utc"] = _epoch_to_iso_utc(
            int(grouped["watermark_source_max_epoch"].max())
            if not grouped.empty
            else None
        )
        return grouped.sort_values(
            by=["event_date", "city_name"], ascending=[True, True]
        ).reset_index(drop=True)

    @staticmethod
    def _record_blob_path(
        *,
        layer: str,
        endpoint: str,
        city: str,
        watermark: Watermark,
        extension: str,
    ) -> str:
        city_slug = _slugify(city)
        return (
            f"{layer}/openweather/{endpoint}/"
            f"ingestion_date={watermark.ingestion_date}/"
            f"ingestion_hour={watermark.ingestion_hour}/"
            f"city={city_slug}/"
            f"run_id={watermark.run_id}.{extension}"
        )

    @staticmethod
    def _dataset_blob_path(
        *,
        layer: str,
        dataset: str,
        watermark: Watermark,
        extension: str,
    ) -> str:
        return (
            f"{layer}/openweather/{dataset}/"
            f"ingestion_date={watermark.ingestion_date}/"
            f"ingestion_hour={watermark.ingestion_hour}/"
            f"run_id={watermark.run_id}/"
            f"part-00000.{extension}"
        )

    def _persist_artifact(
        self,
        *,
        result: PipelineResult,
        layer: str,
        blob_path: str,
        payload: bytes,
        content_type: str,
        watermark: Watermark,
    ) -> None:
        local_path = self.local_sink.write(blob_path, payload)
        result.local_files.append(local_path)

        if self.azure_sink is None:
            return

        metadata = {
            "pipeline": "openweather_medallion",
            "layer": layer,
            "run_id": watermark.run_id,
            "ingestion_ts": watermark.ingestion_ts_utc,
            "ingestion_epoch": str(watermark.ingestion_epoch),
        }
        blob_url = self.azure_sink.upload(
            blob_path=blob_path,
            payload=payload,
            content_type=content_type,
            metadata=metadata,
        )
        result.uploaded_blobs.append(blob_url)
