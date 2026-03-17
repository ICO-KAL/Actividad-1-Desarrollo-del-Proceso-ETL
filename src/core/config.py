from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class WorkerSettings:
    log_level: str
    max_parallel_extractors: int
    temp_output_dir: Path


@dataclass
class DatabaseSettings:
    server: str
    database: str
    driver: str
    trusted_connection: bool
    encrypt: bool
    trust_server_certificate: bool
    username: str | None
    password: str | None


@dataclass
class CsvSourceSettings:
    enabled: bool
    directory: Path
    files: list[str]


@dataclass
class DatabaseQuerySettings:
    name: str
    sql: str


@dataclass
class DatabaseSourceSettings:
    enabled: bool
    queries: list[DatabaseQuerySettings]


@dataclass
class ApiSourceSettings:
    enabled: bool
    base_url: str
    endpoint: str
    timeout_seconds: int
    api_key: str | None


@dataclass
class SourceSettings:
    csv: CsvSourceSettings
    database: DatabaseSourceSettings
    api: ApiSourceSettings


@dataclass
class StagingSettings:
    save_temp_files: bool
    load_to_tables: bool
    clear_before_load: bool
    strict_mode: bool


@dataclass
class AppSettings:
    base_dir: Path
    config_path: Path
    schema_script_path: Path
    worker: WorkerSettings
    database: DatabaseSettings
    sources: SourceSettings
    staging: StagingSettings


def _env(name: str | None) -> str | None:
    if not name:
        return None
    value = os.getenv(name)
    return value if value else None


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    raw = Path(raw_path)
    if raw.is_absolute():
        return raw
    return (base_dir / raw).resolve()


def load_settings(base_dir: Path, config_path: Path | None = None) -> AppSettings:
    base_dir = base_dir.resolve()
    cfg_path = config_path or (base_dir / "config" / "settings.json")

    if not cfg_path.exists():
        raise FileNotFoundError(f"No existe el archivo de configuracion: {cfg_path}")

    data = json.loads(cfg_path.read_text(encoding="utf-8"))

    worker_data = data["worker"]
    db_data = data["database"]
    sources_data = data["sources"]
    staging_data = data["staging"]

    username = _env(db_data.get("username_env"))
    password = _env(db_data.get("password_env"))

    db_settings = DatabaseSettings(
        server=os.getenv("ETL_DB_SERVER", db_data["server"]),
        database=os.getenv("ETL_DB_DATABASE", db_data["database"]),
        driver=os.getenv("ETL_DB_DRIVER", db_data["driver"]),
        trusted_connection=bool(db_data.get("trusted_connection", True)),
        encrypt=bool(db_data.get("encrypt", True)),
        trust_server_certificate=bool(db_data.get("trust_server_certificate", True)),
        username=username,
        password=password,
    )

    csv_data = sources_data["csv"]
    db_source_data = sources_data["database"]
    api_data = sources_data["api"]

    csv_source = CsvSourceSettings(
        enabled=bool(csv_data.get("enabled", True)),
        directory=_resolve_path(base_dir, csv_data["directory"]),
        files=list(csv_data.get("files", [])),
    )

    db_queries = [
        DatabaseQuerySettings(name=item["name"], sql=item["sql"])
        for item in db_source_data.get("queries", [])
    ]

    db_source = DatabaseSourceSettings(
        enabled=bool(db_source_data.get("enabled", True)),
        queries=db_queries,
    )

    api_source = ApiSourceSettings(
        enabled=bool(api_data.get("enabled", True)),
        base_url=api_data["base_url"],
        endpoint=api_data["endpoint"],
        timeout_seconds=int(api_data.get("timeout_seconds", 30)),
        api_key=_env(api_data.get("api_key_env")),
    )

    sources = SourceSettings(csv=csv_source, database=db_source, api=api_source)

    worker_settings = WorkerSettings(
        log_level=str(worker_data.get("log_level", "INFO")),
        max_parallel_extractors=int(worker_data.get("max_parallel_extractors", 3)),
        temp_output_dir=_resolve_path(base_dir, worker_data.get("temp_output_dir", "staging/temp")),
    )

    staging_settings = StagingSettings(
        save_temp_files=bool(staging_data.get("save_temp_files", True)),
        load_to_tables=bool(staging_data.get("load_to_tables", True)),
        clear_before_load=bool(staging_data.get("clear_before_load", True)),
        strict_mode=bool(staging_data.get("strict_mode", False)),
    )

    return AppSettings(
        base_dir=base_dir,
        config_path=cfg_path,
        schema_script_path=(base_dir / "bs" / "ProcesoETL.sql").resolve(),
        worker=worker_settings,
        database=db_settings,
        sources=sources,
        staging=staging_settings,
    )
