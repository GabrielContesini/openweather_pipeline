from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote


NOTEBOOK_MODEL_PATTERN = re.compile(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'")
MAX_UNQUOTE_PASSES = 8


def load_html_export(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"HTML export not found: {path}")
    return path.read_text(encoding="utf-8")


def decode_model_payload(encoded_model: str) -> dict[str, Any]:
    decoded_text = base64.b64decode(encoded_model).decode("utf-8")
    for _ in range(MAX_UNQUOTE_PASSES):
        next_text = unquote(decoded_text)
        if next_text == decoded_text:
            break
        decoded_text = next_text
    return json.loads(decoded_text)


def parse_notebook_model(html_text: str) -> dict[str, Any]:
    match = NOTEBOOK_MODEL_PATTERN.search(html_text)
    if not match:
        raise ValueError("Databricks notebook model not found in HTML export.")
    return decode_model_payload(match.group(1))


def extract_exit_payload(model: dict[str, Any]) -> dict[str, Any]:
    commands = model.get("commands", [])
    if not isinstance(commands, list):
        raise ValueError("Notebook model has invalid 'commands' format.")

    for command in reversed(commands):
        if not isinstance(command, dict):
            continue
        results = command.get("results")
        if not isinstance(results, dict):
            continue
        raw_data = results.get("data")
        if not isinstance(raw_data, str):
            continue
        raw_data = raw_data.strip()
        if not raw_data.startswith("{") or not raw_data.endswith("}"):
            continue
        try:
            payload = json.loads(raw_data)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and {"status", "stage"} <= payload.keys():
            return payload

    raise ValueError("Could not find notebook exit payload in HTML export.")


def safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def build_summary(model: dict[str, Any], payload: dict[str, Any], html_path: Path) -> dict[str, Any]:
    quality = safe_dict(payload.get("quality_report"))
    sla = safe_dict(payload.get("sla_report"))
    delta = safe_dict(payload.get("delta_report"))
    timings = safe_dict(payload.get("timings"))

    return {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_export_file": html_path.as_posix(),
        "notebook_name": model.get("name"),
        "status": payload.get("status"),
        "stage": payload.get("stage"),
        "run_id": payload.get("run_id"),
        "ingestion_date": payload.get("ingestion_date"),
        "ingestion_hour": payload.get("ingestion_hour"),
        "record_counts": {
            "raw_records": payload.get("raw_records"),
            "bronze_records": payload.get("bronze_records"),
            "silver_rows": payload.get("silver_rows"),
            "gold_rows": payload.get("gold_rows"),
        },
        "quality": {
            "passed": quality.get("passed"),
            "failed_checks": quality.get("failed_checks", []),
        },
        "sla": {
            "passed": sla.get("passed"),
            "failed_checks": sla.get("failed_checks", []),
        },
        "delta": {"enabled": delta.get("enabled")},
        "timings": timings,
        "paths": {
            "storage_preflight_path": payload.get("storage_preflight_path"),
            "silver_blob_path": payload.get("silver_blob_path"),
            "gold_blob_path": payload.get("gold_blob_path"),
            "manifest_path": payload.get("manifest_path"),
            "observability_path": payload.get("observability_path"),
        },
        "credential_sources": {
            "openweather_api_key_source": payload.get("openweather_api_key_source"),
            "storage_auth_source": payload.get("storage_auth_source"),
        },
        "active_profile": payload.get("active_profile"),
    }


def default_output_path(html_path: Path) -> Path:
    return html_path.with_suffix(".summary.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract a compact run summary JSON from Databricks notebook HTML export."
    )
    parser.add_argument("html_export", type=Path, help="Path to the exported notebook HTML.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output path. Default: same file name with .summary.json suffix.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    html_path: Path = args.html_export
    output_path: Path = args.output or default_output_path(html_path)

    try:
        html_text = load_html_export(html_path)
        notebook_model = parse_notebook_model(html_text)
        payload = extract_exit_payload(notebook_model)
        summary = build_summary(notebook_model, payload, html_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(summary, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:  # pragma: no cover - cli safety
        print(f"Failed to extract run summary: {exc}", file=sys.stderr)
        return 1

    print(f"Summary written to: {output_path.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
