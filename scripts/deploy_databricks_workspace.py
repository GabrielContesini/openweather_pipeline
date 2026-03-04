from __future__ import annotations

import base64
import os
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator

import requests


ALLOWED_NOTEBOOK_SUFFIXES = {
    ".py": "PYTHON",
    ".sql": "SQL",
    ".scala": "SCALA",
    ".r": "R",
}


def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _normalize_host(raw_host: str) -> str:
    host = raw_host.strip()
    if not host:
        raise ValueError("DATABRICKS_HOST is required.")
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"https://{host}"
    return host.rstrip("/")


def _normalize_workspace_path(raw_path: str, default: str) -> str:
    path = (raw_path or default).strip()
    if not path:
        raise ValueError("DATABRICKS_TARGET_PATH cannot be empty.")
    if not path.startswith("/"):
        path = f"/{path}"
    return path.rstrip("/")


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required.")
    return value


@dataclass(frozen=True)
class DeployConfig:
    host: str
    token: str
    source_dir: Path
    target_path: str
    dry_run: bool

    @classmethod
    def from_env(cls) -> "DeployConfig":
        host = _normalize_host(_required_env("DATABRICKS_HOST"))
        token = _required_env("DATABRICKS_TOKEN")
        source_dir = Path(os.getenv("DATABRICKS_SOURCE_DIR", "notebooks/databricks")).resolve()
        target_path = _normalize_workspace_path(
            os.getenv("DATABRICKS_TARGET_PATH", ""),
            default="/Shared/openweather_pipeline/notebooks/databricks",
        )
        dry_run = _to_bool(os.getenv("DATABRICKS_DRY_RUN"), default=False)
        return cls(
            host=host,
            token=token,
            source_dir=source_dir,
            target_path=target_path,
            dry_run=dry_run,
        )


class DatabricksWorkspaceClient:
    def __init__(self, *, host: str, token: str) -> None:
        self.host = host
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _request(self, *, method: str, api_path: str, json_payload: dict | None = None) -> dict:
        response = self.session.request(
            method=method,
            url=f"{self.host}{api_path}",
            json=json_payload,
            timeout=60,
        )

        if response.status_code >= 400:
            details = response.text.strip()
            raise RuntimeError(
                f"Databricks API error {response.status_code} on {api_path}: {details}"
            )

        if not response.text:
            return {}
        return response.json()

    def mkdirs(self, workspace_path: str) -> None:
        self._request(
            method="POST",
            api_path="/api/2.0/workspace/mkdirs",
            json_payload={"path": workspace_path},
        )

    def import_notebook(
        self,
        *,
        workspace_path: str,
        source_bytes: bytes,
        language: str,
        overwrite: bool = True,
    ) -> None:
        encoded_content = base64.b64encode(source_bytes).decode("utf-8")
        self._request(
            method="POST",
            api_path="/api/2.0/workspace/import",
            json_payload={
                "path": workspace_path,
                "format": "SOURCE",
                "language": language,
                "content": encoded_content,
                "overwrite": overwrite,
            },
        )


def iter_notebooks(source_dir: Path) -> Iterator[tuple[Path, str]]:
    for path in sorted(source_dir.rglob("*")):
        if not path.is_file():
            continue
        language = ALLOWED_NOTEBOOK_SUFFIXES.get(path.suffix.lower())
        if language:
            yield path, language


def build_workspace_notebook_path(
    *, target_root: str, source_dir: Path, local_file: Path
) -> str:
    relative_path = local_file.relative_to(source_dir)
    relative_without_suffix = relative_path.with_suffix("")
    return str(PurePosixPath(target_root) / PurePosixPath(relative_without_suffix.as_posix()))


def deploy_workspace_notebooks(config: DeployConfig) -> int:
    if not config.source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {config.source_dir}")
    if not config.source_dir.is_dir():
        raise NotADirectoryError(f"Source path is not a directory: {config.source_dir}")

    notebooks = list(iter_notebooks(config.source_dir))
    if not notebooks:
        raise ValueError(
            f"No notebooks found under {config.source_dir} "
            f"for suffixes: {', '.join(sorted(ALLOWED_NOTEBOOK_SUFFIXES))}"
        )

    print(f"Host: {config.host}")
    print(f"Source dir: {config.source_dir}")
    print(f"Target path: {config.target_path}")
    print(f"Notebook count: {len(notebooks)}")
    print(f"Dry run: {config.dry_run}")

    if config.dry_run:
        for local_file, _ in notebooks:
            remote_path = build_workspace_notebook_path(
                target_root=config.target_path,
                source_dir=config.source_dir,
                local_file=local_file,
            )
            print(f"[DRY RUN] {local_file} -> {remote_path}")
        return len(notebooks)

    client = DatabricksWorkspaceClient(host=config.host, token=config.token)
    client.mkdirs(config.target_path)

    deployed_count = 0
    for local_file, language in notebooks:
        remote_path = build_workspace_notebook_path(
            target_root=config.target_path,
            source_dir=config.source_dir,
            local_file=local_file,
        )
        parent_dir = str(PurePosixPath(remote_path).parent)
        client.mkdirs(parent_dir)

        source_bytes = local_file.read_bytes()
        client.import_notebook(
            workspace_path=remote_path,
            source_bytes=source_bytes,
            language=language,
            overwrite=True,
        )
        deployed_count += 1
        print(f"[OK] {local_file.name} -> {remote_path}")

    return deployed_count


def main() -> int:
    try:
        config = DeployConfig.from_env()
        deployed_count = deploy_workspace_notebooks(config)
    except Exception as exc:
        print(f"Workspace deploy failed: {exc}")
        return 1

    print(f"Workspace deploy completed. notebooks_deployed={deployed_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
