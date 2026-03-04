from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from settings import PipelineSettings, SettingsError  # noqa: E402


def _write_env_file(tmp_dir: Path, lines: list[str]) -> Path:
    env_path = tmp_dir / ".env.test"
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return env_path


class PipelineSettingsTests(unittest.TestCase):
    def test_missing_api_key_raises_settings_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            env_file = _write_env_file(
                tmp_dir,
                [
                    "OPENWEATHER_CITIES=Sao Paulo,BR;Curitiba,BR",
                ],
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(SettingsError):
                    PipelineSettings.from_env(
                        env_file,
                        require_azure_credentials=False,
                    )

    def test_require_azure_credentials_without_storage_auth_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            env_file = _write_env_file(
                tmp_dir,
                [
                    "OPENWEATHER_API_KEY=test-key",
                    "OPENWEATHER_CITIES=Sao Paulo,BR;Curitiba,BR",
                ],
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(SettingsError):
                    PipelineSettings.from_env(env_file, require_azure_credentials=True)

    def test_local_only_config_loads_without_azure_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            env_file = _write_env_file(
                tmp_dir,
                [
                    "OPENWEATHER_API_KEY=test-key",
                    "OPENWEATHER_CITIES=Sao Paulo,BR;Rio de Janeiro,BR;Curitiba,BR",
                    "OPENWEATHER_ENDPOINTS=weather",
                ],
            )
            with patch.dict(os.environ, {}, clear=True):
                settings = PipelineSettings.from_env(
                    env_file,
                    require_azure_credentials=False,
                )

        self.assertEqual(settings.openweather_api_key, "test-key")
        self.assertEqual(settings.cities, ("Sao Paulo,BR", "Rio de Janeiro,BR", "Curitiba,BR"))
        self.assertFalse(settings.has_azure_credentials)

    def test_account_key_config_enables_azure_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            env_file = _write_env_file(
                tmp_dir,
                [
                    "OPENWEATHER_API_KEY=test-key",
                    "AZURE_STORAGE_ACCOUNT_NAME=tropowxdlprod",
                    "AZURE_STORAGE_CONTAINER=openweather-data",
                    "AZURE_STORAGE_ACCOUNT_KEY=storage-key-value",
                ],
            )
            with patch.dict(os.environ, {}, clear=True):
                settings = PipelineSettings.from_env(
                    env_file,
                    require_azure_credentials=True,
                )

        self.assertEqual(settings.azure_storage_account_name, "tropowxdlprod")
        self.assertEqual(settings.azure_storage_container, "openweather-data")
        self.assertEqual(settings.azure_storage_account_key, "storage-key-value")
        self.assertTrue(settings.has_azure_credentials)


if __name__ == "__main__":
    unittest.main(verbosity=2)
