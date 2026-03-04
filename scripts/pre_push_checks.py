from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_98 = ROOT / "notebooks" / "databricks" / "98_full_pipeline_no_widgets.py"

COMPILE_TARGETS = [
    "notebooks/databricks/_common.py",
    "notebooks/databricks/00_smoke_test.py",
    "notebooks/databricks/01_raw_bronze_ingestion.py",
    "notebooks/databricks/02_silver_transform.py",
    "notebooks/databricks/03_gold_transform.py",
    "notebooks/databricks/98_full_pipeline_no_widgets.py",
    "src/settings.py",
    "src/openweather_client.py",
    "src/medallion_pipeline.py",
    "src/extract_data.py",
]

FORBIDDEN_TRACKED_FILES = [
    "config/.env",
    "config/databricks_free.local.json",
]


def run_command(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def check_syntax() -> list[str]:
    result = run_command(["uv", "run", "python", "-m", "py_compile", *COMPILE_TARGETS])
    if result.returncode == 0:
        return []
    return [f"py_compile failed:\n{result.stderr.strip() or result.stdout.strip()}"]


def check_notebook_plaintext_credentials() -> list[str]:
    if not NOTEBOOK_98.exists():
        return [f"Notebook not found: {NOTEBOOK_98}"]

    content = NOTEBOOK_98.read_text(encoding="utf-8")
    failures: list[str] = []

    for field_name in ("openweather_api_key", "storage_credential"):
        pattern = rf'"{field_name}"\s*:\s*"([^"]*)"'
        for match in re.finditer(pattern, content):
            value = match.group(1).strip()
            if not value:
                continue
            if value.startswith("secret://"):
                continue
            if value.startswith("<") and value.endswith(">"):
                continue
            failures.append(
                f"Potential plaintext credential in notebook 98 field '{field_name}': '{value[:12]}...'"
            )

    return failures


def check_forbidden_tracked_files() -> list[str]:
    failures: list[str] = []
    for relative_path in FORBIDDEN_TRACKED_FILES:
        result = run_command(["git", "ls-files", "--error-unmatch", relative_path])
        if result.returncode == 0:
            failures.append(
                f"Sensitive file is tracked in Git and must be removed: {relative_path}"
            )
    return failures


def main() -> int:
    failures: list[str] = []
    failures.extend(check_syntax())
    failures.extend(check_notebook_plaintext_credentials())
    failures.extend(check_forbidden_tracked_files())

    if failures:
        print("Pre-push checks failed:\n")
        for index, failure in enumerate(failures, start=1):
            print(f"{index}. {failure}")
        return 1

    print("Pre-push checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
