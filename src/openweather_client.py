from __future__ import annotations

import logging
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class OpenWeatherResponse:
    endpoint: str
    city_query: str
    request_url: str
    status_code: int
    payload: dict


class OpenWeatherClient:
    def __init__(self, *, base_url: str, api_key: str, timeout_seconds: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.session = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        retry_config = Retry(
            total=5,
            backoff_factor=1.2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry_config))
        session.mount("http://", HTTPAdapter(max_retries=retry_config))
        return session

    def get_weather(
        self, *, endpoint: str, city: str, units: str, lang: str
    ) -> OpenWeatherResponse:
        url = f"{self.base_url}/{endpoint}"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": units,
            "lang": lang,
        }

        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        status_code = response.status_code
        payload = response.json()
        if status_code >= 400:
            message = payload.get("message", "erro nao especificado")
            raise requests.HTTPError(
                f"OpenWeather retornou HTTP {status_code} para {endpoint}/{city}: {message}",
                response=response,
            )

        LOGGER.info(
            "OpenWeather coletado endpoint=%s cidade=%s status=%s",
            endpoint,
            city,
            status_code,
        )
        return OpenWeatherResponse(
            endpoint=endpoint,
            city_query=city,
            request_url=response.url,
            status_code=status_code,
            payload=payload,
        )
