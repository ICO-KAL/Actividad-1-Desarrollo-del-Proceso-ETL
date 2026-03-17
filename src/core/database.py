from __future__ import annotations

import logging
import re
import urllib.parse
from pathlib import Path

import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.core.config import DatabaseSettings


def build_odbc_connection_string(
    settings: DatabaseSettings,
    database_override: str | None = None,
) -> str:
    database = database_override or settings.database

    parts: list[str] = [
        f"DRIVER={{{settings.driver}}};",
        f"SERVER={settings.server};",
        f"DATABASE={database};",
    ]

    if settings.trusted_connection and not settings.username:
        parts.append("Trusted_Connection=yes;")
    else:
        if not settings.username or not settings.password:
            raise ValueError(
                "Faltan credenciales para SQL Server. Defina ETL_DB_USER y ETL_DB_PASSWORD."
            )
        parts.append(f"UID={settings.username};")
        parts.append(f"PWD={settings.password};")

    parts.append(f"Encrypt={'yes' if settings.encrypt else 'no'};")
    parts.append(
        f"TrustServerCertificate={'yes' if settings.trust_server_certificate else 'no'};"
    )

    return "".join(parts)


def create_sqlalchemy_engine(settings: DatabaseSettings) -> Engine:
    odbc_str = build_odbc_connection_string(settings)
    params = urllib.parse.quote_plus(odbc_str)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        future=True,
    )


def split_sql_batches(script_text: str) -> list[str]:
    return [
        batch.strip()
        for batch in re.split(r"(?im)^\s*GO\s*;?\s*$", script_text)
        if batch.strip()
    ]


def apply_schema_sql(
    script_path: Path,
    settings: DatabaseSettings,
    logger: logging.Logger,
) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"No existe el script SQL: {script_path}")

    script_text = script_path.read_text(encoding="utf-8")
    batches = split_sql_batches(script_text)

    conn_str = build_odbc_connection_string(settings=settings, database_override="master")

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        for index, batch in enumerate(batches, start=1):
            cursor.execute(batch)
            if index % 10 == 0:
                logger.info("[SQL] Lotes ejecutados: %s/%s", index, len(batches))

    logger.info("[SQL] Esquema aplicado correctamente en la base %s.", settings.database)
