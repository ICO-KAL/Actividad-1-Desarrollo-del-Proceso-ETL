from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from src.core.config import DatabaseSettings, DatabaseSourceSettings
from src.core.contracts import ExtractedDataset, IExtractor
from src.core.database import create_sqlalchemy_engine


class DatabaseExtractor(IExtractor):
    name = "DatabaseExtractor"
    source_type = "database"

    def __init__(
        self,
        db_settings: DatabaseSettings,
        source_settings: DatabaseSourceSettings,
        logger: logging.Logger,
    ) -> None:
        self._db_settings = db_settings
        self._source_settings = source_settings
        self._logger = logger

    async def extract(self) -> list[ExtractedDataset]:
        return await asyncio.to_thread(self._extract_sync)

    def _extract_sync(self) -> list[ExtractedDataset]:
        extracted_at = datetime.now(timezone.utc).isoformat()
        datasets: list[ExtractedDataset] = []

        engine = create_sqlalchemy_engine(self._db_settings)
        try:
            with engine.connect() as conn:
                for query in self._source_settings.queries:
                    dataframe = pd.read_sql(text(query.sql), conn)
                    dataframe["_query_name"] = query.name
                    dataframe["_extracted_at"] = extracted_at

                    datasets.append(
                        ExtractedDataset(
                            extractor_name=self.name,
                            source_type=self.source_type,
                            entity_name=query.name,
                            source_detail=query.sql,
                            dataframe=dataframe,
                        )
                    )

                    self._logger.info(
                        "[DatabaseExtractor] Query %s extrajo %s filas.",
                        query.name,
                        len(dataframe),
                    )
        finally:
            engine.dispose()

        return datasets
