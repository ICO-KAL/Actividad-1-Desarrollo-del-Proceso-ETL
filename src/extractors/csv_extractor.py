from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.core.config import CsvSourceSettings
from src.core.contracts import ExtractedDataset, IExtractor


class CsvExtractor(IExtractor):
    name = "CsvExtractor"
    source_type = "csv"

    def __init__(self, settings: CsvSourceSettings, logger: logging.Logger) -> None:
        self._settings = settings
        self._logger = logger

    async def extract(self) -> list[ExtractedDataset]:
        return await asyncio.to_thread(self._extract_sync)

    def _extract_sync(self) -> list[ExtractedDataset]:
        extracted_at = datetime.now(timezone.utc).isoformat()
        datasets: list[ExtractedDataset] = []

        for file_name in self._settings.files:
            csv_path = self._settings.directory / file_name
            if not csv_path.exists():
                self._logger.warning("[CsvExtractor] Archivo no encontrado: %s", csv_path)
                continue

            dataframe = pd.read_csv(csv_path)
            dataframe["_source_file"] = file_name
            dataframe["_extracted_at"] = extracted_at

            datasets.append(
                ExtractedDataset(
                    extractor_name=self.name,
                    source_type=self.source_type,
                    entity_name=Path(file_name).stem,
                    source_detail=str(csv_path),
                    dataframe=dataframe,
                )
            )

            self._logger.info(
                "[CsvExtractor] Archivo %s extraido con %s filas.",
                file_name,
                len(dataframe),
            )

        return datasets
