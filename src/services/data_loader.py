from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.core.contracts import ExtractedDataset, ExtractionMetric


class StagingDataLoader:
    _TABLE_MAP = {
        "csv": "StgCsvExtract",
        "database": "StgDbExtract",
        "api": "StgApiExtract",
    }

    def __init__(self, engine: Engine, logger: logging.Logger, temp_dir: Path) -> None:
        self._engine = engine
        self._logger = logger
        self._temp_dir = temp_dir

    def clear_staging_tables(self) -> None:
        cleanup_sql = text(
            """
            DELETE FROM dbo.ExtractionAudit;
            DELETE FROM dbo.StgApiExtract;
            DELETE FROM dbo.StgDbExtract;
            DELETE FROM dbo.StgCsvExtract;
            """
        )
        with self._engine.begin() as conn:
            conn.execute(cleanup_sql)
        self._logger.info("[DataLoader] Tablas staging y auditoria limpiadas.")

    def write_temp_files(self, batch_id: UUID, datasets: list[ExtractedDataset]) -> list[Path]:
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        created_files: list[Path] = []

        for dataset in datasets:
            safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", dataset.entity_name)
            file_name = f"{batch_id}_{dataset.source_type}_{safe_name}.jsonl"
            file_path = self._temp_dir / file_name
            dataset.dataframe.to_json(file_path, orient="records", lines=True, force_ascii=False)
            created_files.append(file_path)

        self._logger.info("[DataLoader] Archivos temporales generados: %s", len(created_files))
        return created_files

    def load_to_staging_tables(self, batch_id: UUID, datasets: list[ExtractedDataset]) -> None:
        for dataset in datasets:
            table_name = self._TABLE_MAP.get(dataset.source_type)
            if not table_name:
                continue

            staging_df = self._to_staging_frame(batch_id=batch_id, dataset=dataset)
            if staging_df.empty:
                continue

            staging_df.to_sql(
                name=table_name,
                con=self._engine,
                schema="dbo",
                if_exists="append",
                index=False,
                chunksize=500,
            )

            self._logger.info(
                "[DataLoader] %s filas cargadas en dbo.%s.",
                len(staging_df),
                table_name,
            )

    def persist_metrics(self, batch_id: UUID, metrics: list[ExtractionMetric]) -> None:
        if not metrics:
            return

        now = datetime.now(timezone.utc)
        metrics_df = pd.DataFrame(
            [
                {
                    "BatchId": str(batch_id),
                    "Component": metric.extractor_name,
                    "SourceType": metric.source_type,
                    "EntityName": metric.entity_name,
                    "RowsExtracted": metric.row_count,
                    "DurationMs": metric.duration_ms,
                    "Status": metric.status,
                    "ErrorMessage": metric.error_message,
                    "CreatedAt": now,
                }
                for metric in metrics
            ]
        )

        metrics_df.to_sql(
            name="ExtractionAudit",
            con=self._engine,
            schema="dbo",
            if_exists="append",
            index=False,
            chunksize=500,
        )

        self._logger.info("[DataLoader] Metricas de auditoria guardadas: %s", len(metrics_df))

    def _to_staging_frame(self, batch_id: UUID, dataset: ExtractedDataset) -> pd.DataFrame:
        if dataset.dataframe.empty:
            return pd.DataFrame()

        now = datetime.now(timezone.utc)
        records: list[dict[str, object]] = []

        for _, row in dataset.dataframe.iterrows():
            payload = self._sanitize_record(row.to_dict())
            records.append(
                {
                    "BatchId": str(batch_id),
                    "EntityName": dataset.entity_name,
                    "SourceDetail": dataset.source_detail,
                    "PayloadJson": json.dumps(payload, ensure_ascii=False),
                    "ExtractedAt": now,
                }
            )

        return pd.DataFrame(records)

    @staticmethod
    def _sanitize_record(record: dict[str, object]) -> dict[str, object]:
        sanitized: dict[str, object] = {}

        for key, value in record.items():
            if value is None:
                sanitized[key] = None
                continue

            if pd.isna(value):
                sanitized[key] = None
                continue

            if isinstance(value, (datetime, date)):
                sanitized[key] = value.isoformat()
                continue

            if hasattr(value, "item"):
                sanitized[key] = value.item()
                continue

            sanitized[key] = value

        return sanitized
