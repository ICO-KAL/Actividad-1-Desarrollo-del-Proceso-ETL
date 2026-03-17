from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from time import perf_counter
from uuid import UUID, uuid4

from sqlalchemy.engine import Engine

from src.core.config import AppSettings
from src.core.contracts import ExtractedDataset, ExtractionMetric, IExtractor
from src.core.database import apply_schema_sql, create_sqlalchemy_engine
from src.extractors.api_extractor import ApiExtractor
from src.extractors.csv_extractor import CsvExtractor
from src.extractors.database_extractor import DatabaseExtractor
from src.services.data_loader import StagingDataLoader


@dataclass
class WorkerExecutionResult:
    batch_id: UUID
    total_datasets: int
    total_rows: int
    errors: int


class EtlWorkerService:
    def __init__(self, settings: AppSettings, logger: logging.Logger) -> None:
        self._settings = settings
        self._logger = logger
        self._engine: Engine = create_sqlalchemy_engine(settings.database)
        self._loader = StagingDataLoader(
            engine=self._engine,
            logger=logger,
            temp_dir=settings.worker.temp_output_dir,
        )
        self._extractors = self._build_extractors()
        self._semaphore = asyncio.Semaphore(settings.worker.max_parallel_extractors)

    async def run(self, run_schema: bool = True) -> WorkerExecutionResult:
        batch_id = uuid4()
        self._logger.info("=== Inicio Worker ETL (batch=%s) ===", batch_id)

        if run_schema:
            apply_schema_sql(
                script_path=self._settings.schema_script_path,
                settings=self._settings.database,
                logger=self._logger,
            )

        if self._settings.staging.clear_before_load and self._settings.staging.load_to_tables:
            self._loader.clear_staging_tables()

        extractor_runs = await asyncio.gather(
            *[self._run_extractor(extractor) for extractor in self._extractors]
        )

        datasets: list[ExtractedDataset] = []
        metrics: list[ExtractionMetric] = []

        for extracted_datasets, extracted_metrics in extractor_runs:
            datasets.extend(extracted_datasets)
            metrics.extend(extracted_metrics)

        if self._settings.staging.save_temp_files:
            self._loader.write_temp_files(batch_id=batch_id, datasets=datasets)

        if self._settings.staging.load_to_tables:
            self._loader.load_to_staging_tables(batch_id=batch_id, datasets=datasets)
            self._loader.persist_metrics(batch_id=batch_id, metrics=metrics)

        total_rows = sum(len(dataset.dataframe) for dataset in datasets)
        total_errors = sum(1 for metric in metrics if metric.status != "OK")

        self._logger.info("Datasets extraidos: %s", len(datasets))
        self._logger.info("Total de filas extraidas: %s", total_rows)
        self._logger.info("Errores detectados: %s", total_errors)
        self._logger.info("=== Fin Worker ETL (batch=%s) ===", batch_id)

        if total_errors > 0 and self._settings.staging.strict_mode:
            raise RuntimeError("Se detectaron errores de extraccion en modo estricto.")

        return WorkerExecutionResult(
            batch_id=batch_id,
            total_datasets=len(datasets),
            total_rows=total_rows,
            errors=total_errors,
        )

    async def _run_extractor(
        self,
        extractor: IExtractor,
    ) -> tuple[list[ExtractedDataset], list[ExtractionMetric]]:
        async with self._semaphore:
            started = perf_counter()
            try:
                datasets = await extractor.extract()
                duration_ms = int((perf_counter() - started) * 1000)

                if not datasets:
                    return [], [
                        ExtractionMetric(
                            extractor_name=extractor.name,
                            source_type=extractor.source_type,
                            entity_name="none",
                            row_count=0,
                            duration_ms=duration_ms,
                            status="OK",
                        )
                    ]

                metrics = [
                    ExtractionMetric(
                        extractor_name=extractor.name,
                        source_type=extractor.source_type,
                        entity_name=dataset.entity_name,
                        row_count=len(dataset.dataframe),
                        duration_ms=duration_ms,
                        status="OK",
                    )
                    for dataset in datasets
                ]

                return datasets, metrics
            except Exception as exc:
                duration_ms = int((perf_counter() - started) * 1000)
                self._logger.exception("Extractor %s fallo: %s", extractor.name, exc)
                return [], [
                    ExtractionMetric(
                        extractor_name=extractor.name,
                        source_type=extractor.source_type,
                        entity_name="error",
                        row_count=0,
                        duration_ms=duration_ms,
                        status="ERROR",
                        error_message=str(exc),
                    )
                ]

    def _build_extractors(self) -> list[IExtractor]:
        extractors: list[IExtractor] = []

        if self._settings.sources.csv.enabled:
            extractors.append(CsvExtractor(self._settings.sources.csv, self._logger))

        if self._settings.sources.database.enabled:
            extractors.append(
                DatabaseExtractor(
                    db_settings=self._settings.database,
                    source_settings=self._settings.sources.database,
                    logger=self._logger,
                )
            )

        if self._settings.sources.api.enabled:
            extractors.append(ApiExtractor(self._settings.sources.api, self._logger))

        if not extractors:
            raise ValueError("No hay extractores habilitados en la configuracion.")

        return extractors
