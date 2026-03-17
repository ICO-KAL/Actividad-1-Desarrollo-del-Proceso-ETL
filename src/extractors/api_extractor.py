from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx
import pandas as pd

from src.core.config import ApiSourceSettings
from src.core.contracts import ExtractedDataset, IExtractor


class ApiExtractor(IExtractor):
    name = "ApiExtractor"
    source_type = "api"

    def __init__(self, settings: ApiSourceSettings, logger: logging.Logger) -> None:
        self._settings = settings
        self._logger = logger

    async def extract(self) -> list[ExtractedDataset]:
        headers: dict[str, str] = {}
        if self._settings.api_key:
            headers["Authorization"] = f"Bearer {self._settings.api_key}"

        async with httpx.AsyncClient(
            base_url=self._settings.base_url,
            timeout=self._settings.timeout_seconds,
        ) as client:
            response = await client.get(self._settings.endpoint, headers=headers)
            response.raise_for_status()
            payload = response.json()

        if isinstance(payload, list):
            dataframe = pd.json_normalize(payload)
        elif isinstance(payload, dict):
            dataframe = pd.json_normalize([payload])
        else:
            dataframe = pd.DataFrame({"value": [str(payload)]})

        extracted_at = datetime.now(timezone.utc).isoformat()
        dataframe["_endpoint"] = self._settings.endpoint
        dataframe["_extracted_at"] = extracted_at

        self._logger.info(
            "[ApiExtractor] Endpoint %s extrajo %s filas.",
            self._settings.endpoint,
            len(dataframe),
        )

        return [
            ExtractedDataset(
                extractor_name=self.name,
                source_type=self.source_type,
                entity_name="api_payload",
                source_detail=f"{self._settings.base_url}{self._settings.endpoint}",
                dataframe=dataframe,
            )
        ]
