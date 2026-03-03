from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from medallion_pipeline import OpenWeatherMedallionPipeline
from settings import PipelineSettings, SettingsError


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pipeline medallhao OpenWeather -> Azure Storage "
            "(raw/bronze/silver/gold) com watermark."
        )
    )
    parser.add_argument(
        "--env-file",
        default="config/.env",
        help="Caminho do arquivo .env com configuracoes do pipeline.",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Executa somente escrita local, sem upload no Azure Storage.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Nivel de log.",
    )
    return parser.parse_args()


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> int:
    args = _parse_args()
    _configure_logging(args.log_level)

    try:
        settings = PipelineSettings.from_env(
            Path(args.env_file),
            require_azure_credentials=not args.local_only,
        )
        pipeline = OpenWeatherMedallionPipeline(
            settings=settings,
            local_only=args.local_only,
        )
        result = pipeline.run()
    except SettingsError as exc:
        logging.error("Erro de configuracao: %s", exc)
        return 2
    except Exception as exc:  # pragma: no cover
        logging.exception("Falha na execucao do pipeline: %s", exc)
        return 1

    logging.info("Pipeline finalizado. run_id=%s", result.run_id)
    logging.info(
        "Camadas: raw=%s bronze=%s silver=%s gold=%s",
        result.raw_records,
        result.bronze_records,
        result.silver_rows,
        result.gold_rows,
    )
    logging.info(
        "Arquivos locais=%s | Blobs enviados=%s",
        len(result.local_files),
        len(result.uploaded_blobs),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
