from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv


class SettingsError(ValueError):
    """Raised when required environment settings are missing."""


def _parse_list(
    raw_value: str | None, *, default: Iterable[str], separator: str = ","
) -> tuple[str, ...]:
    if not raw_value:
        return tuple(default)
    return tuple(item.strip() for item in raw_value.split(separator) if item.strip())


def _parse_int(raw_value: str | None, *, default: int) -> int:
    if raw_value is None:
        return default
    return int(raw_value)


@dataclass(frozen=True)
class PipelineSettings:
    openweather_api_key: str
    cities: tuple[str, ...]
    openweather_base_url: str
    openweather_units: str
    openweather_lang: str
    openweather_endpoints: tuple[str, ...]
    request_timeout_seconds: int
    azure_storage_account_name: str
    azure_storage_account_url: str
    azure_storage_container: str
    azure_storage_connection_string: str | None
    azure_storage_account_key: str | None
    azure_storage_sas_token: str | None
    local_output_dir: Path

    @property
    def has_azure_credentials(self) -> bool:
        return bool(
            self.azure_storage_connection_string
            or self.azure_storage_account_key
            or self.azure_storage_sas_token
        )

    @classmethod
    def from_env(
        cls,
        env_file: Path = Path("config/.env"),
        *,
        require_azure_credentials: bool = True,
    ) -> "PipelineSettings":
        if env_file.exists():
            load_dotenv(env_file)

        openweather_api_key = os.getenv("OPENWEATHER_API_KEY") or os.getenv("API_KEY")
        if not openweather_api_key:
            raise SettingsError(
                "Defina OPENWEATHER_API_KEY (ou API_KEY) no ambiente/config/.env."
            )

        cities = _parse_list(
            os.getenv("OPENWEATHER_CITIES"),
            default=("Sao Paulo,BR", "Rio de Janeiro,BR", "Curitiba,BR"),
            separator=";",
        )
        if not cities:
            raise SettingsError("Defina ao menos uma cidade em OPENWEATHER_CITIES.")

        azure_storage_account_name = os.getenv(
            "AZURE_STORAGE_ACCOUNT_NAME", "tropowxdlprod"
        ).strip()
        azure_storage_account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL", "").strip()
        if not azure_storage_account_name and require_azure_credentials:
            raise SettingsError("Defina AZURE_STORAGE_ACCOUNT_NAME.")
        if not azure_storage_account_name:
            azure_storage_account_name = "local-only"
        if not azure_storage_account_url:
            azure_storage_account_url = (
                f"https://{azure_storage_account_name}.blob.core.windows.net"
            )

        azure_storage_container = os.getenv(
            "AZURE_STORAGE_CONTAINER", "openweather-data"
        ).strip()
        if not azure_storage_container:
            raise SettingsError("Defina AZURE_STORAGE_CONTAINER.")

        settings = cls(
            openweather_api_key=openweather_api_key,
            cities=cities,
            openweather_base_url=os.getenv(
                "OPENWEATHER_BASE_URL", "https://api.openweathermap.org/data/2.5"
            ).strip(),
            openweather_units=os.getenv("OPENWEATHER_UNITS", "metric").strip(),
            openweather_lang=os.getenv("OPENWEATHER_LANG", "pt_br").strip(),
            openweather_endpoints=_parse_list(
                os.getenv("OPENWEATHER_ENDPOINTS"),
                default=("weather",),
                separator=",",
            ),
            request_timeout_seconds=_parse_int(
                os.getenv("OPENWEATHER_TIMEOUT_SECONDS"), default=30
            ),
            azure_storage_account_name=azure_storage_account_name,
            azure_storage_account_url=azure_storage_account_url,
            azure_storage_container=azure_storage_container,
            azure_storage_connection_string=os.getenv(
                "AZURE_STORAGE_CONNECTION_STRING"
            ),
            azure_storage_account_key=os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
            azure_storage_sas_token=os.getenv("AZURE_STORAGE_SAS_TOKEN"),
            local_output_dir=Path(os.getenv("LOCAL_OUTPUT_DIR", "data")).resolve(),
        )

        if require_azure_credentials and not settings.has_azure_credentials:
            raise SettingsError(
                "Defina AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_ACCOUNT_KEY "
                "ou AZURE_STORAGE_SAS_TOKEN para upload no Azure Storage."
            )

        return settings
