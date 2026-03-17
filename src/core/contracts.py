from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class ExtractedDataset:
    extractor_name: str
    source_type: str
    entity_name: str
    source_detail: str
    dataframe: pd.DataFrame


@dataclass
class ExtractionMetric:
    extractor_name: str
    source_type: str
    entity_name: str
    row_count: int
    duration_ms: int
    status: str
    error_message: str | None = None


class IExtractor(ABC):
    name: str
    source_type: str

    @abstractmethod
    async def extract(self) -> list[ExtractedDataset]:
        raise NotImplementedError
